use anyhow::Result;
use db_worker::{DbWorker, delete_old_messages};
use manager::WorkerPoolManager;
use metrics::{ScalingAction, apply_adaptive_scaling_strategy};
use pool::WorkerPoolConfig;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info};

mod db_worker;
mod manager;
mod metrics;
mod pool;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Setup database
    let db_pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(10)
        .connect("sqlite:data.db")
        .await?;

    // Create worker pool manager
    let manager =
        Arc::new(WorkerPoolManager::new("amqp://guest:guest@localhost:5672", db_pool).await?);

    let pool_configs = WorkerPoolConfig {
        queue_name: "tasks".to_string(),
        min_workers: 5,
        max_workers: 20,
        prefetch_count: 10,
    };

    // Add worker pools
    manager
        .add_pool(pool_configs, {
            let db_pool = manager.db_pool();
            move || Arc::new(DbWorker::new(db_pool.clone()))
        })
        .await?;

    // Start a background task to monitor load and scale pools
    tokio::spawn(handle_pool_scaling(manager.clone()));

    // Setup separate thread for database queries
    tokio::spawn(delete_old_messages(manager.db_pool()));

    // Keep the application running
    tokio::signal::ctrl_c().await?;
    info!("Shutting down");

    Ok(())
}

async fn handle_pool_scaling(manager: Arc<WorkerPoolManager>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

    loop {
        interval.tick().await;

        // Get metrics for all pools
        let pool_names: Vec<String> = {
            let pools = manager.pools.read().await;
            pools.keys().cloned().collect()
        };

        // Process each pool
        for queue_name in pool_names {
            // Get internal metrics
            let pool_metrics = { manager.metrics.read().await.get(&queue_name).cloned() };

            if let Some(metrics) = pool_metrics {
                // Apply adaptive scaling strategy
                let scaling_action = apply_adaptive_scaling_strategy(&queue_name, &metrics);

                // Apply the scaling decision
                match scaling_action {
                    ScalingAction::ScaleUp(count) => {
                        info!(
                            "Auto-scaling up pool {} by {} workers (queue_depth={}, rate={:.2} msg/s, lag={}ms, util={}%)",
                            queue_name,
                            count,
                            metrics.queue_depth,
                            metrics.processing_rate,
                            metrics.avg_processing_time_ms,
                            metrics.utilization
                        );
                        if let Err(e) = manager.scale_up(&queue_name, count).await {
                            error!("Failed to scale up pool {}: {}", queue_name, e);
                        }
                    }
                    ScalingAction::ScaleDown(count) => {
                        info!(
                            "Auto-scaling down pool {} by {} workers (queue_depth={}, rate={:.2} msg/s, lag={}ms, util={}%)",
                            queue_name,
                            count,
                            metrics.queue_depth,
                            metrics.processing_rate,
                            metrics.avg_processing_time_ms,
                            metrics.utilization
                        );
                        if let Err(e) = manager.scale_down(&queue_name, count).await {
                            error!("Failed to scale down pool {}: {}", queue_name, e);
                        }
                    }
                    ScalingAction::NoChange => {
                        debug!(
                            "No scaling needed for pool {}: queue_depth={}, rate={:.2} msg/s, lag={}ms, workers={}/{}, util={}%",
                            queue_name,
                            metrics.queue_depth,
                            metrics.processing_rate,
                            metrics.avg_processing_time_ms,
                            metrics.active_workers,
                            metrics.max_workers,
                            metrics.utilization,
                        );
                    }
                }

                // Update last scale time in metrics if needed
                if scaling_action != ScalingAction::NoChange {
                    if let Some(metrics) = manager.metrics.write().await.get_mut(&queue_name) {
                        metrics.last_scale_time = Instant::now();
                    }
                }
            }
        }
    }
}
