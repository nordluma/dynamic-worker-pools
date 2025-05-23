use std::{
    cmp::min,
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use futures::{StreamExt, TryStreamExt};
use lapin::{
    Channel, Connection, ConnectionProperties, Consumer,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
};
use sqlx::{Pool, Sqlite};
use tokio::{
    sync::{RwLock, Semaphore, mpsc},
    task::JoinHandle,
    time::sleep,
};
use tracing::{Span, debug, error, info, instrument, warn};

use crate::{
    metrics::{
        PoolMetrics, get_queue_metrics, init_metrics, record_message_processed, refresh_metrics,
        update_pool_metrics,
    },
    pool::{PoolCommand, Worker, WorkerMetric, WorkerPool, WorkerPoolConfig},
};

// Worker pool manager that handles scaling of multiple pools
pub struct WorkerPoolManager {
    pub pools: RwLock<HashMap<String, WorkerPool>>,
    pub connection: Arc<Connection>,
    pub metrics: Arc<RwLock<HashMap<String, PoolMetrics>>>,
    pub db_pool: Arc<Pool<Sqlite>>,
}

impl WorkerPoolManager {
    pub async fn new(amqp_url: &str, db_pool: Pool<Sqlite>) -> Result<Self> {
        // Connect to RabbitMQ
        let connection = Connection::connect(amqp_url, ConnectionProperties::default()).await?;

        info!("Connected to RabbitMQ");

        Ok(Self {
            pools: RwLock::new(HashMap::new()),
            connection: Arc::new(connection),
            db_pool: Arc::new(db_pool),
            metrics: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    // Add a new worker pool
    #[instrument(
        name = "WorkerPoolManager::add_pool",
        skip_all,
        fields(queue.name = config.queue_name)
    )]
    pub async fn add_pool<F>(&self, config: WorkerPoolConfig, worker_factory: F) -> Result<()>
    where
        F: Fn() -> Arc<dyn Worker> + Send + Sync + 'static,
    {
        let worker_factory = Arc::new(worker_factory);
        let queue_name = config.queue_name.clone();

        // Check if pool already exists
        if self.pools.read().await.contains_key(&queue_name) {
            anyhow::bail!("Pool for queue {} already exists", queue_name);
        }

        let (command_tx, command_rx) = mpsc::channel(100);
        let (metrics_tx, metrics_rx) = mpsc::channel(1000);

        // Create channel for this pool
        let channel = self.connection.create_channel().await?;
        channel
            .basic_qos(config.prefetch_count, BasicQosOptions::default())
            .await?;

        // Declare the queue
        let queue_info = channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        info!("Declared queue: {}", queue_name);

        // Initialize metrics for this pool
        let message_count = queue_info.message_count();
        let initial_metrics = PoolMetrics::new(config.max_workers, message_count);

        self.metrics
            .write()
            .await
            .insert(queue_name.clone(), initial_metrics);

        // Initialize global metrics
        init_metrics(&queue_name, config.max_workers, message_count);

        // Start metrics collection task
        let metrics_clone = self.metrics.clone();
        let queue_name_clone = queue_name.clone();
        let connection_clone = self.connection.clone();

        tokio::spawn(Self::run_metrics_collector(
            metrics_rx,
            metrics_clone,
            queue_name_clone,
            connection_clone,
        ));

        let config_clone = config.clone();
        // Start the pool manager task
        let pool_handle = tokio::spawn(Self::run_pool(
            config_clone,
            channel,
            worker_factory.clone(),
            command_rx,
            metrics_tx.clone(),
        ));

        // Store the pool
        let pool = WorkerPool {
            config,
            active_workers: 0,
            command_tx,
            _pool_handle: pool_handle,
        };

        self.pools.write().await.insert(queue_name.clone(), pool);
        info!("Added worker pool for queue: {}", queue_name);

        Ok(())
    }

    // Scale up a specific pool
    #[instrument(
        name = "WorkerPoolManager::scale_up",
        skip_all,
        fields(queue.name = queue_name, count)
    )]
    pub async fn scale_up(&self, queue_name: &str, count: usize) -> Result<()> {
        let pools = self.pools.read().await;

        if let Some(pool) = pools.get(queue_name) {
            pool.command_tx
                .send(PoolCommand::ScaleUp(count))
                .await
                .map_err(|_| anyhow::anyhow!("Failed to send scale up command"))?;

            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Pool for queue {} does not exist",
                queue_name
            ))
        }
    }

    // Scale down a specific pool
    #[instrument(
        name = "WorkerPoolManager::scale_down",
        skip_all,
        fields(queue.name = queue_name, count)
    )]
    pub async fn scale_down(&self, queue_name: &str, count: usize) -> Result<()> {
        let pools = self.pools.read().await;

        if let Some(pool) = pools.get(queue_name) {
            pool.command_tx
                .send(PoolCommand::ScaleDown(count))
                .await
                .map_err(|_| anyhow::anyhow!("Failed to send scale down command"))?;
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Pool for queue {} does not exist",
                queue_name
            ))
        }
    }

    // Get current worker count for a pool
    #[allow(unused)]
    pub async fn get_worker_count(&self, queue_name: &str) -> Result<usize> {
        let pools = self.pools.read().await;

        if let Some(pool) = pools.get(queue_name) {
            Ok(pool.active_workers)
        } else {
            Err(anyhow::anyhow!(
                "Pool for queue {} does not exist",
                queue_name
            ))
        }
    }

    // Remove a pool and shut it down
    #[allow(unused)]
    pub async fn remove_pool(&self, queue_name: &str) -> Result<()> {
        let mut pools = self.pools.write().await;

        if let Some(pool) = pools.get(queue_name) {
            // Send shutdown command
            pool.command_tx
                .send(PoolCommand::Shutdown)
                .await
                .map_err(|_| anyhow::anyhow!("Failed to send shutdown command"))?;

            // Remove from pools map
            pools.remove(queue_name);
            info!("Removed worker pool for queue: {}", queue_name);

            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Pool for queue {} does not exist",
                queue_name
            ))
        }
    }

    // Get database connection pool
    pub fn db_pool(&self) -> Arc<Pool<Sqlite>> {
        self.db_pool.clone()
    }

    /// Run metrics collector for a pool
    #[instrument(
        name = "WorkerPoolManager::run_metrics_collector",
        skip_all,
        fields(
            queue.name = queue_name,
            workers.active = 0,
        )
    )]
    async fn run_metrics_collector(
        mut metrics_rx: mpsc::Receiver<WorkerMetric>,
        metrics_storage: Arc<RwLock<HashMap<String, PoolMetrics>>>,
        queue_name: String,
        connection: Arc<Connection>,
    ) {
        let mut total_processing_time = 0;
        let mut processed_count = 0;
        let mut active_workers = 0usize;

        // Start a periodic task to refresh time-windowed metrics
        let refresh_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));

            loop {
                interval.tick().await;
                refresh_metrics();
            }
        });

        // Periodically update queue depth from RabbitMQ
        let queue_depth_updater = {
            let metrics_storage = metrics_storage.clone();
            let queue_name = queue_name.clone();

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));

                loop {
                    interval.tick().await;

                    let current_metrics =
                        { metrics_storage.read().await.get(&queue_name).cloned() };

                    match get_queue_metrics(&queue_name, current_metrics.as_ref()).await {
                        Ok(queue_metrics) => {
                            // Update metrics with the collected data
                            if let Some(metrics) =
                                metrics_storage.write().await.get_mut(&queue_name)
                            {
                                metrics.queue_depth = queue_metrics.queue_depth;
                                metrics.utilization = queue_metrics.utilization;

                                // Update processing rate if we don't have our
                                // own calculation
                                if metrics.processing_rate == 0.0 {
                                    metrics.processing_rate = queue_metrics.processing_rate;
                                }

                                // Update processing lag
                                // if metrics.avg_processing_time_ms == 0 {
                                metrics.avg_processing_time_ms = queue_metrics.processing_lag_ms;
                                // }

                                update_pool_metrics(&queue_name, metrics);
                                Span::current().record("workers.active", metrics.active_workers);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to get queue metrics for {queue_name}: {e}");

                            // Get current queue depth
                            match connection.create_channel().await {
                                Ok(channel) => {
                                    match channel
                                        .queue_declare(
                                            &queue_name,
                                            QueueDeclareOptions {
                                                passive: true,
                                                ..QueueDeclareOptions::default()
                                            },
                                            FieldTable::default(),
                                        )
                                        .await
                                    {
                                        Ok(queue_info) => {
                                            let queue_depth = queue_info.message_count();

                                            // Update metrics
                                            if let Some(metrics) =
                                                metrics_storage.write().await.get_mut(&queue_name)
                                            {
                                                metrics.queue_depth = queue_depth;
                                                Span::current().record(
                                                    "workers.active",
                                                    metrics.active_workers,
                                                );

                                                // Recalculate utilization basd on queue depth
                                                if metrics.active_workers > 0 {
                                                    if queue_depth > 0 {
                                                        metrics.utilization = f64::min(
                                                            100.0,
                                                            60.0 + (queue_depth as f64
                                                                / metrics.active_workers as f64)
                                                                * 20.0,
                                                        );
                                                    } else {
                                                        metrics.utilization = 20.0;
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Failed to get queue info for {}: {}",
                                                queue_name, e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to create channel for metrics collection: {}",
                                        e
                                    );
                                    sleep(Duration::from_secs(5)).await;
                                }
                            }
                        }
                    }
                }
            })
        };

        // Process worker metrics
        while let Some(metric) = metrics_rx.recv().await {
            // Update metrics based on events
            match metric {
                WorkerMetric::MessageProcessed { processing_time_ms } => {
                    total_processing_time += processing_time_ms;
                    processed_count += 1;

                    record_message_processed(&queue_name, processing_time_ms);
                    // Update metrics storage with processed message
                    if let Some(metrics) = metrics_storage.write().await.get_mut(&queue_name) {
                        metrics.add_processed_message(processing_time_ms);
                    }
                }
                WorkerMetric::MessageFailed => {
                    if let Some(metrics) = metrics_storage.write().await.get_mut(&queue_name) {
                        metrics.failed_messages += 1;
                    }
                }
                WorkerMetric::WorkerStarted => {
                    active_workers += 1;
                    if let Some(metrics) = metrics_storage.write().await.get_mut(&queue_name) {
                        info!("setting active workers to {active_workers}");
                        metrics.active_workers = active_workers;
                        Span::current().record("workers.active", active_workers);
                    }
                }
                WorkerMetric::WorkerStopped => {
                    if active_workers > 0 {
                        active_workers -= 1;
                    }
                    if let Some(metrics) = metrics_storage.write().await.get_mut(&queue_name) {
                        metrics.active_workers = active_workers;
                    }
                }
            }
        }

        // Cancel updater tasks when metrics collector exits
        queue_depth_updater.abort();
        refresh_task.abort();
    }

    // Run a worker pool
    #[instrument(
        name = "WorkerPoolManager::run_pool",
        skip_all,
        fields(queue.name = config.queue_name)
    )]
    async fn run_pool(
        config: WorkerPoolConfig,
        channel: Channel,
        worker_factory: Arc<dyn Fn() -> Arc<dyn Worker> + Send + Sync>,
        mut command_rx: mpsc::Receiver<PoolCommand>,
        metrics_tx: mpsc::Sender<WorkerMetric>,
    ) {
        let channel = Arc::new(channel);
        let queue_name = config.queue_name.clone();

        // Track worker handles
        let workers = Arc::new(RwLock::new(Vec::<JoinHandle<()>>::new()));

        // Control access to consumer with a semaphore
        let sem = Arc::new(Semaphore::new(config.max_workers));

        // Create consumer
        let consumer = match channel
            .basic_consume(
                &queue_name,
                &format!("consumer-{}", uuid::Uuid::new_v4()),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
        {
            Ok(consumer) => consumer,
            Err(e) => {
                error!("Failed to create consumer for queue {}: {}", queue_name, e);
                return;
            }
        };

        let consumer = Arc::new(RwLock::new(consumer));

        // Start initial minimum workers
        for _ in 0..config.min_workers {
            Self::spawn_worker(
                &queue_name,
                &worker_factory,
                &workers,
                &sem,
                consumer.clone(),
                channel.clone(),
                metrics_tx.clone(),
            )
            .await;
        }

        // Cooldown timer for scaling operations
        let mut last_scale_time = Instant::now();
        let scale_cooldown = Duration::from_secs(30);

        // Process commands
        while let Some(cmd) = command_rx.recv().await {
            match cmd {
                PoolCommand::ScaleUp(count) => {
                    // Check cooldown period
                    if last_scale_time.elapsed() < scale_cooldown {
                        debug!("Skipping scale up due to cooldown period");
                        continue;
                    }

                    let current = workers.read().await.len();
                    let to_add = min(count, config.max_workers - current);

                    if to_add > 0 {
                        for _ in 0..to_add {
                            Self::spawn_worker(
                                &queue_name,
                                &worker_factory,
                                &workers,
                                &sem,
                                consumer.clone(),
                                channel.clone(),
                                metrics_tx.clone(),
                            )
                            .await;
                        }

                        last_scale_time = Instant::now();
                        info!(
                            "Scaled up pool {} by {} workers (now at {})",
                            queue_name,
                            to_add,
                            current + to_add
                        );
                    } else {
                        debug!("Pool {} already at max capacity ({})", queue_name, current);
                    }
                }

                PoolCommand::ScaleDown(count) => {
                    // Check cooldown period
                    if last_scale_time.elapsed() < scale_cooldown {
                        debug!("Skipping scale down due to cooldown period");
                        continue;
                    }

                    let mut workers_lock = workers.write().await;
                    let current = workers_lock.len();
                    let to_remove = min(count, current - config.min_workers);

                    if to_remove > 0 {
                        for _ in 0..to_remove {
                            if let Some(handle) = workers_lock.pop() {
                                handle.abort();
                                // Report worker stopped
                                let _ = metrics_tx.send(WorkerMetric::WorkerStopped).await;
                            }
                        }

                        last_scale_time = Instant::now();
                        info!(
                            "Scaled down pool {} by {} workers (now at {})",
                            queue_name,
                            to_remove,
                            current - to_remove
                        );
                    } else {
                        debug!(
                            "Pool {} already at minimum capacity ({})",
                            queue_name, current
                        );
                    }
                }

                PoolCommand::Shutdown => {
                    info!("Shutting down pool {}", queue_name);
                    let mut workers_lock = workers.write().await;

                    // Abort all workers
                    for handle in workers_lock.drain(..) {
                        handle.abort();
                        // Report worker stopped
                        let _ = metrics_tx.send(WorkerMetric::WorkerStopped).await;
                    }

                    break;
                }
            }
        }
    }

    /// Spawn a new worker
    #[instrument(
        name = "WorkerPoolManager::spawn_worker",
        skip_all,
        fields(queue.name = queue_name)
    )]
    async fn spawn_worker(
        queue_name: &str,
        worker_factory: &Arc<dyn Fn() -> Arc<dyn Worker> + Send + Sync>,
        workers: &Arc<RwLock<Vec<JoinHandle<()>>>>,
        sem: &Arc<Semaphore>,
        consumer: Arc<RwLock<Consumer>>,
        channel: Arc<Channel>,
        metrics_tx: mpsc::Sender<WorkerMetric>,
    ) {
        let queue_name = queue_name.to_string();
        let worker = worker_factory();
        let sem = sem.clone();
        let consumer = consumer.clone();
        let channel = channel.clone();
        let metrics_tx = metrics_tx.clone();

        // Spawn the worker task
        let handle = tokio::spawn(async move {
            // Acquire semaphore permit
            let _permit = match sem.acquire().await {
                Ok(permit) => permit,
                Err(e) => {
                    error!("Failed to acquire semaphore permit: {}", e);
                    return;
                }
            };

            let _ = metrics_tx.send(WorkerMetric::WorkerStarted).await;
            debug!("Started worker for queue {}", queue_name);

            let consumer = { consumer.write().await.clone() };

            run_worker(worker, channel, consumer, metrics_tx.clone()).await;

            let _ = metrics_tx.send(WorkerMetric::WorkerStopped).await;
            debug!("Worker for queue {} exited", queue_name);
        });

        // Store the worker handle
        workers.write().await.push(handle);
    }
}

#[instrument(name = "run_worker", skip_all)]
async fn run_worker(
    worker: Arc<dyn Worker>,
    channel: Arc<Channel>,
    consumer: Consumer,
    metrics_tx: mpsc::Sender<WorkerMetric>,
) {
    let mut consumer_stream = consumer.into_stream();

    while let Some(delivery) = consumer_stream.next().await {
        match delivery {
            Ok(delivery) => {
                let delivery_tag = delivery.delivery_tag;
                let start_time = Instant::now();

                // Process the message
                match worker.process(delivery).await {
                    Ok(_) => {
                        let processing_time_ms = start_time.elapsed().as_millis() as u64;

                        // Report successful processing with timing
                        let _ = metrics_tx
                            .send(WorkerMetric::MessageProcessed { processing_time_ms })
                            .await;

                        // Acknowledge the message
                        if let Err(e) = channel
                            .basic_ack(delivery_tag, BasicAckOptions::default())
                            .await
                        {
                            error!("Failed to acknowledge message: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Failed to process message: {}", e);

                        // Report failure
                        let _ = metrics_tx.send(WorkerMetric::MessageFailed).await;

                        // Negative acknowledge the message
                        if let Err(e) = channel
                            .basic_nack(delivery_tag, BasicNackOptions::default())
                            .await
                        {
                            error!("Failed to negative acknowledge message: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to receive message: {}", e);
            }
        }
    }
}
