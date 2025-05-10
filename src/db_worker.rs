use std::sync::Arc;

use anyhow::Result;
use lapin::message::Delivery;
use sqlx::{Pool, Sqlite};
use tracing::debug;

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
    async fn process(&self, msg: Delivery) -> Result<()> {
        let data = String::from_utf8(msg.data.clone())?;
        debug!("Processing message: {}", data);

        // Process the message and write to the database
        sqlx::query!("INSERT INTO messages (content) VALUES (?)", data)
            .execute(self.db_pool.as_ref())
            .await?;

        Ok(())
    }
}
