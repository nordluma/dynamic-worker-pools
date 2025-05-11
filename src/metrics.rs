use std::{
    cmp::{max, min},
    collections::{HashMap, VecDeque},
    sync::RwLock,
    time::{Duration, Instant},
};

use anyhow::Result;
use lapin::{Connection, ConnectionProperties, options::QueueDeclareOptions, types::FieldTable};
use tracing::{Span, instrument};

lazy_static::lazy_static! {
    pub static ref GLOBAL_METRICS: RwLock<HashMap<String, PoolMetrics>> = RwLock::new(HashMap::new());
}

#[derive(Clone, Debug)]
pub struct PoolMetrics {
    pub active_workers: usize,
    pub max_workers: usize,
    pub queue_depth: u32,
    pub utilization: f64,
    pub processed_messages: u64,
    pub processed_messages_last_minute: u64,
    pub last_scale_time: Instant,
    pub failed_messages: u64,
    pub avg_processing_time_ms: u64,
    /// Messages processed per second
    pub processing_rate: f64,
    /// Time and processing duration for recent messages
    pub message_history: VecDeque<(Instant, u64)>,
    pub last_metrics_update: Instant,
}

impl PoolMetrics {
    pub fn new(max_workers: usize, queue_depth: u32) -> Self {
        Self {
            active_workers: 0,
            max_workers,
            queue_depth,
            utilization: 0.0,
            processed_messages: 0,
            processed_messages_last_minute: 0,
            last_scale_time: Instant::now(),
            failed_messages: 0,
            avg_processing_time_ms: 0,
            processing_rate: 0.0,
            message_history: VecDeque::with_capacity(100), // Keep the last 100 processing times
            last_metrics_update: Instant::now(),
        }
    }

    /// Calculate processing rate based on message history
    #[instrument(name = "PoolMetrics::update_processing_rate", skip_all)]
    pub fn update_processing_rate(&mut self) {
        // Remove messages older than 30 seconds
        let cutoff = Instant::now() - Duration::from_secs(30);
        while let Some((time, _)) = self.message_history.front() {
            if *time < cutoff {
                self.message_history.pop_front();
            } else {
                break;
            }
        }

        // Calculate messages per second based on recent history
        if !self.message_history.is_empty() {
            let oldest = self.message_history.front().unwrap().0;
            let newest = self.message_history.back().unwrap().0;
            let time_span = newest.duration_since(oldest).as_secs_f64();

            if time_span > 0.0 {
                self.processing_rate = self.message_history.len() as f64 / time_span;
            }
        } else {
            self.processing_rate = 0.0;
        }
    }

    /// Add processed message to history
    #[instrument(name = "PoolMetrics::add_processed_message", skip_all)]
    pub fn add_processed_message(&mut self, processing_time_ms: u64) {
        self.processed_messages += 1;
        self.message_history
            .push_back((Instant::now(), processing_time_ms));

        // Update running average of processing time
        if self.avg_processing_time_ms == 0 {
            self.avg_processing_time_ms = processing_time_ms;
        } else {
            // Weighted average - 80% old value, 20% new value
            self.avg_processing_time_ms =
                (self.avg_processing_time_ms * 4 + processing_time_ms) / 5;
        }

        // Update processing rate if it's been at least 5 seconds
        if self.last_metrics_update.elapsed() > Duration::from_secs(5) {
            self.update_processing_rate();
            self.last_metrics_update = Instant::now();
        }
    }

    /// Update metrics that need periodic refreshing
    pub fn update_time_window_metrics(&mut self) {
        let now = Instant::now();

        // Reset the "last minute" counter if a minute has passed
        if now.duration_since(self.last_metrics_update) >= Duration::from_secs(60) {
            self.processed_messages_last_minute = 0;
            self.last_metrics_update = now;
        }
    }
}

/// Queue metrics used for scaling decisions
pub struct QueueMetrics {
    /// Number of messages in queue
    pub queue_depth: u32,

    /// Messages processed per second
    pub processing_rate: f64,

    /// Worker utilization percentage
    pub utilization: f64,

    /// Average processing time per message in ms
    pub processing_lag_ms: u64,
}

/// Initialize global metrics
#[instrument(name = "init_metrics")]
pub fn init_metrics(queue_name: &str, max_workers: usize, queue_depth: u32) {
    let mut metrics = GLOBAL_METRICS.write().unwrap();
    metrics.insert(
        queue_name.to_string(),
        PoolMetrics::new(max_workers, queue_depth),
    );
}

/// Update global metrics when a message is processed
pub fn record_message_processed(queue_name: &str, processing_time_ms: u64) {
    if let Ok(mut metrics) = GLOBAL_METRICS.write() {
        if let Some(pool_metrics) = metrics.get_mut(queue_name) {
            pool_metrics.add_processed_message(processing_time_ms);
        }
    }
}

// Periodically refresh time-windowed metrics
#[instrument(name = "refresh_metrics")]
pub fn refresh_metrics() {
    if let Ok(mut metrics) = GLOBAL_METRICS.write() {
        for (_, pool_metrics) in metrics.iter_mut() {
            pool_metrics.update_time_window_metrics();
        }
    }
}

pub fn update_pool_metrics(queue_name: &str, metrics: &PoolMetrics) {
    if let Ok(mut global_metrics) = GLOBAL_METRICS.write() {
        if let Some(pool_metrics) = global_metrics.get_mut(queue_name) {
            // Update with the latest worker pool metrics
            pool_metrics.active_workers = metrics.active_workers;
            pool_metrics.max_workers = metrics.max_workers;
            pool_metrics.queue_depth = metrics.queue_depth;
            pool_metrics.utilization = metrics.utilization;
            pool_metrics.failed_messages = metrics.failed_messages;
            pool_metrics.last_scale_time = metrics.last_scale_time;

            // We don't overwrite `processed_messages_last_minute` as that's
            // tracked independently
        } else {
            // If queue not found, add it
            global_metrics.insert(queue_name.to_string(), metrics.clone());
        }
    }
}

/// Get metrics for a specific queue from RabbitMQ
#[instrument(name = "get_queue_metrics", skip_all, fields(queue.name = queue_name))]
pub async fn get_queue_metrics(
    queue_name: &str,
    _metrics: Option<&PoolMetrics>,
) -> Result<QueueMetrics> {
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

    // Get queue depth (number of message waiting)
    let queue_depth = queue_info.message_count();

    // Calculate real metrics from our pool metrics if available
    // let (processing_rate, utilization, processing_lag_ms) = if let Some(metrics) = metrics {
    //     // Calculate actual processing rate from our metrics
    //     let rate = metrics.processing_rate;
    //
    //     // Calculate utilization
    //     let utilization = if metrics.active_workers > 0 {
    //         if queue_depth > 0 {
    //             // Higher utilization when there are messages in the queue
    //             f64::min(
    //                 100.0,
    //                 60.0 + (queue_depth as f64 / metrics.active_workers as f64) * 20.0,
    //             )
    //         } else {
    //             // Lower utilization when queue is empty
    //             20.0
    //         }
    //     } else {
    //         0.0
    //     };
    //
    //     let lag_ms = if rate > 0.0 {
    //         ((queue_depth as f64 / rate) * 1000.0) as u64
    //     } else if metrics.avg_processing_time_ms > 0 {
    //         queue_depth as u64 * metrics.avg_processing_time_ms
    //     } else {
    //         0
    //     };
    //
    //     (rate, utilization, lag_ms)
    // } else {
    //     // Fallback values when we don't have metrics
    //     let processing_rate = 10.0; // Messages pre second per worker
    //     let utilization = if queue_depth > 0 { 80.0 } else { 20.0 }; // Percentage
    //     let processing_lag_ms = if queue_depth > 0 { 200 } else { 50 }; // Milliseconds
    //
    //     (processing_rate, utilization, processing_lag_ms)
    // };
    //
    // Ok(QueueMetrics {
    //     queue_depth,
    //     processing_rate,
    //     utilization,
    //     processing_lag_ms,
    // })

    // Step 1: Get metrics form our application tracking system
    // We'll need to access our metrics storage to get data without processed
    // messages

    // For production, you would have a metrics registry or a way to look up
    // these values
    // For now, let's make this function access the global metrics directly
    let metrics_storage = match GLOBAL_METRICS.read() {
        Ok(metrics) => metrics,
        Err(_) => {
            // Fallback to estimated values if we can't get the metrics
            return Ok(QueueMetrics {
                queue_depth,
                processing_rate: estimate_processing_rate(queue_depth),
                utilization: estimate_utilization(queue_depth),
                processing_lag_ms: estimate_processing_lag(queue_depth),
            });
        }
    };

    // Find metrics for this queue
    if let Some(pool_metrics) = metrics_storage.get(queue_name) {
        // Step 2: Calculate processing rate
        // Processing rate = number of messages processed in last window / time window size
        let time_window = Duration::from_secs(60); // Last minute
        let message_processed_in_window = pool_metrics.processed_messages_last_minute;

        let processing_rate = if message_processed_in_window > 0 {
            message_processed_in_window as f64 / time_window.as_secs_f64()
        } else {
            0.0
        };

        // Step 3: Calculate utilization
        // Utilization = active_workers / max workers * (adjustment based on queue depth)
        let base_utilization = if pool_metrics.max_workers > 0 {
            (pool_metrics.active_workers as f64 / pool_metrics.max_workers as f64) * 100.0
        } else {
            0.0
        };

        // Adjust utilization based on queue depth and processing rate
        let utilization = calculate_utilization(
            base_utilization,
            queue_depth,
            processing_rate,
            pool_metrics.active_workers,
        );

        // Step 4: Calculate processing lag
        // Processing lag = (queue depth / processing rate) in milliseconds
        // This estimates how long it would take to process all messages in the
        // queue
        let processing_lag_ms = if processing_rate > 0.0 {
            ((queue_depth as f64 / processing_rate) * 1000.0) as u64
        } else if queue_depth > 0 {
            // If we can't calculate based on rate, use the average processing
            // time
            pool_metrics.avg_processing_time_ms * queue_depth as u64
        } else {
            0 // No lag if queue is empty
        };

        Ok(QueueMetrics {
            queue_depth,
            processing_rate,
            utilization,
            processing_lag_ms,
        })
    } else {
        // If we can't find metrics for this queue, provide estimated values
        Ok(QueueMetrics {
            queue_depth,
            processing_rate: estimate_processing_rate(queue_depth),
            utilization: estimate_utilization(queue_depth),
            processing_lag_ms: estimate_processing_lag(queue_depth),
        })
    }
}

/// Calculate a weighted utilization based on multiple factors
fn calculate_utilization(
    base_utilization: f64,
    queue_depth: u32,
    processing_rate: f64,
    active_workers: usize,
) -> f64 {
    // Start with base utilization (active/max workers)
    let mut utilization = base_utilization;

    // Adjust based on queue depth
    if queue_depth > 0 {
        // If there are messages waiting, utilization increases
        // The higher the ratio of `queue_depth` to workers, the higher the
        // utilization
        if active_workers > 0 {
            let queue_per_worker = queue_depth as f64 / active_workers as f64;

            // Add up to 40% utilization based on queue size per worker
            let queue_factor = (queue_per_worker / 10.0).min(1.0) * 40.0;
            utilization += queue_factor;
        } else {
            // If no active workers but messages exist, consider high
            // utilization needed
            utilization += 50.0;
        }
    } else {
        // If queue is empty, reduce the utilization
        utilization = utilization.max(20.0) * 0.5;
    }

    // Factor in processing rate - if workers are processing quickly, they're
    // busy
    if processing_rate > 0.0 {
        // Assume a good processing rate is 10 message per second per worker
        let expected_rate = 10.0 * active_workers as f64;
        let rate_ratio = if expected_rate > 0.0 {
            (processing_rate / expected_rate).min(1.5)
        } else {
            0.0
        };

        // Add up to 20% based on processing rate
        utilization += rate_ratio * 20.0;
    }

    // Ensure within bounds
    utilization.max(0.0).min(100.0)
}

/// Estimate processing rate when we don't have metrics
fn estimate_processing_rate(queue_depth: u32) -> f64 {
    // Assume higher processing rate when queue is not empty
    // This is just a placehodler - in reality, this would depend on your
    // workload
    if queue_depth > 0 {
        10.0 // messages per second
    } else {
        // Lower estimated rate when queue is empty
        // (as we're not seeing much activity)
        2.0
    }
}

/// Estimate utilization when we don't have metrics
fn estimate_utilization(queue_depth: u32) -> f64 {
    match queue_depth {
        100.. => 90.0,   // High utilization with large queue
        50..100 => 80.0, // Significant utilization
        10..50 => 70.0,  // Moderate utilization
        1..10 => 50.0,   // Low-moderate utilization
        0 => 20.0,       // Low utilization when queue is empty
    }
}

/// Estimate processing lag when we don't have metrics
fn estimate_processing_lag(queue_depth: u32) -> u64 {
    // Very simple heuristic - assume 100ms per message
    queue_depth as u64 * 100
}

/// Sophisticated adaptive scaling strategy that balances responsiveness with stability
#[instrument(
    name = "apply_adaptive_scaling_strategy",
    skip_all,
    fields(
        queue.name = _queue_name,
        queue.scaling_need = tracing::field::Empty,
        queue.scaling_action = tracing::field::Empty
    )
)]
pub fn apply_adaptive_scaling_strategy(_queue_name: &str, metrics: &PoolMetrics) -> ScalingAction {
    // Don't scale if we've scaled recently (cooldown period)
    if metrics.last_scale_time.elapsed() < Duration::from_secs(30) {
        return ScalingAction::NoChange;
    }

    // Extract metrics
    let queue_depth = metrics.queue_depth;
    let active_workers = metrics.active_workers;
    let max_workers = metrics.max_workers;
    let min_workers = 1; // Get from config
    let avg_process_time_ms = metrics.avg_processing_time_ms;
    let utilization = metrics.utilization;
    let processing_rate = metrics.processing_rate;

    // Calculate processing capacity (messages/sec)
    let processing_capacity = if avg_process_time_ms > 0 {
        active_workers as f64 * (1000.0 / avg_process_time_ms as f64)
    } else {
        active_workers as f64 * 10.0 // Default assumption: 10 msgs/sec per worker
    };

    // Calculate time to drain queue at current capacity
    let time_to_drain_sec = if processing_rate > 0.0 {
        queue_depth as f64 / processing_capacity
    } else {
        0.0
    };

    // Calculate projected queue depth in 1 minute based on current rate
    let projected_growth_rate = processing_rate - processing_capacity;
    let projected_queue_depth = queue_depth as f64 + (projected_growth_rate * 60.0);

    let scaling_need = calculate_scaling_need(
        queue_depth,
        processing_rate,
        processing_capacity,
        utilization,
        time_to_drain_sec,
    );
    Span::current().record("queue.scaling_need", tracing::field::debug(&scaling_need));

    let scaling_action = match scaling_need {
        ScalingNeed::Critical => {
            // Calculate number of workers needed based on processing rates
            //
            // Process projected queue in 30 seconds
            let additional_capacity_needed = (projected_queue_depth / 30.0) as usize;
            let ideal_workers = active_workers + additional_capacity_needed;

            let to_add = min(ideal_workers - active_workers, max_workers);
            if to_add > 0 {
                ScalingAction::ScaleUp(to_add)
            } else {
                ScalingAction::NoChange
            }
        }
        ScalingNeed::High => {
            let to_add = max(3, active_workers / 4); // Add 25% more workers or atleast 3
            ScalingAction::ScaleUp(min(to_add, max_workers - active_workers))
        }
        ScalingNeed::Medium => {
            let to_add = max(2, active_workers / 10); // Add 10% more workers or atleast 2
            ScalingAction::ScaleUp(min(to_add, max_workers - active_workers))
        }
        ScalingNeed::Low => ScalingAction::ScaleUp(min(1, max_workers - active_workers)),
        ScalingNeed::Optimal => ScalingAction::NoChange,
        ScalingNeed::Underutilized => {
            if active_workers > min_workers + 1 {
                ScalingAction::ScaleDown(1)
            } else {
                ScalingAction::NoChange
            }
        }
        ScalingNeed::HighlyUnderutilized => {
            if active_workers > min_workers + 2 {
                let to_remove = max(2, active_workers / 10);
                let target = active_workers - to_remove;
                if target >= min_workers {
                    ScalingAction::ScaleDown(to_remove)
                } else {
                    ScalingAction::ScaleDown(active_workers - min_workers)
                }
            } else {
                ScalingAction::NoChange
            }
        }
    };

    Span::current().record(
        "queue.scaling_action",
        tracing::field::debug(&scaling_action),
    );

    scaling_action
}

/// Scaling decision
#[derive(Debug)]
pub enum ScalingAction {
    ScaleUp(usize),
    ScaleDown(usize),
    NoChange,
}

//b Calculate the scaling need based on various factors
#[derive(Debug)]
enum ScalingNeed {
    /// Need immediate aggressive scaling
    Critical,
    /// Need significant scaling
    High,
    /// Need moderate scaling
    Medium,
    /// Need minor scaling
    Low,
    /// Current scaling is optimal
    Optimal,
    /// Slightly underutilized
    Underutilized,
    /// Significantly underutilized
    HighlyUnderutilized,
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
    processing_rate: f64,
    processing_capacity: f64,
    utilization: f64,
    time_to_drain_sec: f64,
    //avg_process_time_ms: u64,
) -> ScalingNeed {
    // Calculate projected queue depth in 1 minute based on current rate
    let projected_growth_rate = processing_rate - processing_capacity;
    let projected_queue_depth = queue_depth as f64 + (projected_growth_rate * 60.0);

    if projected_queue_depth > 500.0 || time_to_drain_sec > 180.0 {
        // Queue will be very large in 1 minute or it would take > 3 minutes to
        // drain
        ScalingNeed::Critical
    } else if projected_queue_depth > 200.0 || time_to_drain_sec > 120.0 {
        // Queue will be large or it would take > 2 minutes to drain
        ScalingNeed::High
    } else if projected_queue_depth > 50.0 || time_to_drain_sec > 60.0 {
        // Queue will be large or it would take > 1 minute to drain
        ScalingNeed::Medium
    } else if queue_depth > 10 && utilization > 70.0 {
        // Current conditions indicate some scaling needed
        ScalingNeed::Low
    } else if projected_queue_depth < 0.0 && queue_depth < 5 && utilization < 20.0 {
        // Queue is projected to empty and utilization is low
        ScalingNeed::HighlyUnderutilized
    } else if projected_queue_depth < 5.0 && utilization < 40.0 {
        // Queue is projected to remain low and utilization is moderate
        ScalingNeed::Underutilized
    } else {
        // Current scaling seems appropriate
        ScalingNeed::Optimal
    }
}
