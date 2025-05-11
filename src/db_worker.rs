use std::{sync::Arc, time::Duration};

use anyhow::Result;
use lapin::message::Delivery;
use rand::{random_bool, random_range};
use sqlx::{Pool, Sqlite, SqlitePool};
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

pub async fn delete_old_messages(db_pool: Arc<SqlitePool>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

    loop {
        interval.tick().await;

        // Get a connection from the pool
        // Read from the database
        let res =
            sqlx::query!("DELETE FROM messages WHERE created_at < datetime('now', '-5 minutes')")
                .execute(db_pool.as_ref())
                .await;

        if let Ok(res) = res {
            let rows_affected = res.rows_affected();
            if rows_affected > 0 {
                info!("deleted {} messages", rows_affected);
            }
        }
    }
}
