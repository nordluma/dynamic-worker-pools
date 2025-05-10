use std::{sync::Arc, time::Duration};

use anyhow::Result;
use lapin::message::Delivery;
use rand::{random_bool, random_range};
use sqlx::{Pool, Sqlite};
use tracing::{debug, info, instrument};

use crate::pool::Worker;

// Example implementation for a database worker
pub struct DbWorker {
    db_pool: Arc<Pool<Sqlite>>,
}

impl DbWorker {
    pub fn new(db_pool: Arc<Pool<Sqlite>>) -> Self {
        Self { db_pool }
    }
}

#[async_trait::async_trait()]
impl Worker for DbWorker {
    #[instrument(name = "DbWorker::process", skip_all)]
    async fn process(&self, msg: Delivery) -> Result<()> {
        let data = String::from_utf8(msg.data.clone())?;
        debug!("Processing message: {}", data);

        if random_bool(0.33) {
            let duration = random_range(1..5);

            info!("sleeping for {duration} seconds");
            tokio::time::sleep(Duration::from_secs(duration)).await;
        }

        // Process the message and write to the database
        sqlx::query!("INSERT INTO messages (content) VALUES (?)", data)
            .execute(self.db_pool.as_ref())
            .await?;

        Ok(())
    }
}
