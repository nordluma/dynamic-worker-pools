use std::{
    cmp::{max, min},
    time::{Duration, Instant},
};

use anyhow::Result;
use lapin::{Connection, ConnectionProperties, options::QueueDeclareOptions, types::FieldTable};

// Example main function showing usage
// Queue metrics used for scaling decisions
#[allow(unused)]
struct QueueMetrics {
    pub queue_depth: u32,       // Number of messages in queue
    pub processing_rate: f64,   // Messages processed per second
    pub utilization: f64,       // Worker utilization percentage
    pub processing_lag_ms: u64, // Average processing time per message in ms
}

#[derive(Clone, Debug)]
pub struct PoolMetrics {
    pub active_workers: usize,
    pub max_workers: usize,
    pub queue_depth: u32,
    pub utilization: f64,
    pub processed_messages: u64,
    pub last_scale_time: Instant,
    pub failed_messages: u64,
    pub avg_processing_time_ms: u64,
}

// Implement various scaling strategies

// Get metrics for a specific queue from RabbitMQ
#[allow(unused)]
async fn get_queue_metrics(queue_name: &str) -> Result<QueueMetrics> {
    // In a real implementation, you would:
    // 1. Connect to RabbitMQ management API or use your connection to get queue statistics
    // 2. Query your own metrics collection system if you have one

    // This is a simplified example that connects directly to RabbitMQ
    let connection = Connection::connect(
        "amqp://guest:guest@localhost:5672",
        ConnectionProperties::default(),
    )
    .await?;

    let channel = connection.create_channel().await?;

    // Get queue information
    let queue_info = channel
        .queue_declare(
            queue_name,
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;

    // Get worker metrics from your application
    // In this example, we're simulating these values
    // In a real implementation, you would track these metrics within your application

    // Calculate metrics
    let queue_depth = queue_info.message_count();

    // For demonstration, we'll simulate the other metrics
    // In production, you would calculate these from actual measurements
    let processing_rate = 10.0; // Messages per second per worker
    let utilization = if queue_depth > 0 { 80.0 } else { 20.0 }; // Percentage
    let processing_lag_ms = if queue_depth > 0 { 200 } else { 50 }; // Milliseconds

    Ok(QueueMetrics {
        queue_depth,
        processing_rate,
        utilization,
        processing_lag_ms,
    })
}

// Sophisticated adaptive scaling strategy that balances responsiveness with stability
pub fn apply_adaptive_scaling_strategy(_queue_name: &str, metrics: &PoolMetrics) -> ScalingAction {
    // Don't scale if we've scaled recently (cooldown period)
    if metrics.last_scale_time.elapsed() < Duration::from_secs(30) {
        return ScalingAction::NoChange;
    }

    // Calculate metrics that matter for scaling
    let queue_depth = metrics.queue_depth;
    let active_workers = metrics.active_workers;
    let max_workers = metrics.max_workers;
    let avg_process_time_ms = metrics.avg_processing_time_ms;
    let utilization = metrics.utilization;

    // Calculate messages per worker
    let messages_per_worker = if active_workers > 0 {
        queue_depth as f64 / active_workers as f64
    } else {
        queue_depth as f64 // Avoid division by zero
    };

    // Calculate scaling need
    let scaling_need = calculate_scaling_need(
        queue_depth,
        messages_per_worker,
        utilization,
        avg_process_time_ms,
    );

    match scaling_need {
        ScalingNeed::Critical => {
            // Aggressive scaling for critical situations
            let to_add = max(5, active_workers / 2); // Add 50% more workers or at least 5
            ScalingAction::ScaleUp(min(to_add, max_workers - active_workers))
        }
        ScalingNeed::High => {
            // Significant scaling
            let to_add = max(3, active_workers / 4); // Add 25% more workers or at least 3
            ScalingAction::ScaleUp(min(to_add, max_workers - active_workers))
        }
        ScalingNeed::Medium => {
            // Moderate scaling
            let to_add = max(2, active_workers / 10); // Add 10% more workers or at least 2
            ScalingAction::ScaleUp(min(to_add, max_workers - active_workers))
        }
        ScalingNeed::Low => {
            // Minimal scaling
            ScalingAction::ScaleUp(min(1, max_workers - active_workers))
        }
        ScalingNeed::Optimal => {
            // No change needea
            ScalingAction::NoChange
        }
        ScalingNeed::Underutilized => {
            // Scale down slightly
            if active_workers > 1 {
                ScalingAction::ScaleDown(1)
            } else {
                ScalingAction::NoChange
            }
        }
        ScalingNeed::HighlyUnderutilized => {
            // Scale down more aggressively
            let to_remove = max(2, active_workers / 10); // Remove 10% of workers or at least 2
            ScalingAction::ScaleDown(to_remove)
        }
    }
}

// Scaling decision
pub enum ScalingAction {
    ScaleUp(usize),
    ScaleDown(usize),
    NoChange,
}

// Calculate the scaling need based on various factors
enum ScalingNeed {
    Critical,            // Need immediate aggressive scaling
    High,                // Need significant scaling
    Medium,              // Need moderate scaling
    Low,                 // Need minor scaling
    Optimal,             // Current scaling is optimal
    Underutilized,       // Slightly underutilized
    HighlyUnderutilized, // Significantly underutilized
}

// Implement operator overloads for ScalingAction
impl PartialEq for ScalingAction {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ScalingAction::ScaleUp(a), ScalingAction::ScaleUp(b)) => a == b,
            (ScalingAction::ScaleDown(a), ScalingAction::ScaleDown(b)) => a == b,
            (ScalingAction::NoChange, ScalingAction::NoChange) => true,
            _ => false,
        }
    }
}

fn calculate_scaling_need(
    queue_depth: u32,
    messages_per_worker: f64,
    utilization: f64,
    avg_process_time_ms: u64,
) -> ScalingNeed {
    // Define thresholds based on your workload characteristics
    if queue_depth > 500 || (queue_depth > 200 && avg_process_time_ms > 1000) {
        // Very high queue depth or high queue with slow processing
        return ScalingNeed::Critical;
    }

    if queue_depth > 200 || (queue_depth > 100 && messages_per_worker > 20.0) {
        // High queue depth or medium queue with high per-worker load
        return ScalingNeed::High;
    }

    if queue_depth > 50 || (utilization > 80.0 && queue_depth > 20) {
        // Medium queue depth or high utilization with some queue buildup
        return ScalingNeed::Medium;
    }

    if queue_depth > 10 && utilization > 70.0 {
        // Small queue with high utilization
        return ScalingNeed::Low;
    }

    if queue_depth < 5 && utilization < 20.0 {
        // Very low queue and utilization
        return ScalingNeed::HighlyUnderutilized;
    }

    if queue_depth < 10 && utilization < 40.0 {
        // Low queue and utilization
        return ScalingNeed::Underutilized;
    }

    // Default - current scaling is fine
    ScalingNeed::Optimal
}
