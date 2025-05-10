use anyhow::Result;
use lapin::message::Delivery;
use tokio::{sync::mpsc, task::JoinHandle};

// Trait for worker implementations
#[async_trait::async_trait()]
pub trait Worker: Send + Sync + 'static {
    async fn process(&self, msg: Delivery) -> Result<()>;
}

// Configuration for a worker pool
#[derive(Debug, Clone)]
pub struct WorkerPoolConfig {
    pub queue_name: String,
    pub min_workers: usize,
    pub max_workers: usize,
    pub prefetch_count: u16,
}

// Representation of a single worker pool
pub struct WorkerPool {
    pub config: WorkerPoolConfig,
    //pub worker_factory: Arc<dyn Fn() -> Arc<dyn Worker> + Send + Sync>,
    pub active_workers: usize,
    pub command_tx: mpsc::Sender<PoolCommand>,
    pub metrics_tx: mpsc::Sender<WorkerMetric>,
    pub _pool_handle: JoinHandle<()>,
}

#[derive(Debug)]
pub enum WorkerMetric {
    MessageProcessed { processing_time_ms: u64 },
    MessageFailed,
    WorkerStarted,
    WorkerStopped,
}

// Commands to control a worker pool
pub enum PoolCommand {
    ScaleUp(usize),
    ScaleDown(usize),
    Shutdown,
    ReportMetrics,
}
