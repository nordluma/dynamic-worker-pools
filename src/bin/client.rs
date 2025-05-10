use std::time::Duration;

use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicPublishOptions, BasicQosOptions},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection = Connection::connect(
        "amqp://guest:guest@localhost:5672",
        ConnectionProperties::default(),
    )
    .await?;

    let channel = connection.create_channel().await?;
    channel.basic_qos(10, BasicQosOptions::default()).await?;

    loop {
        eprintln!("Sending a batch of messages");
        for _ in 0..10 {
            channel
                .basic_publish(
                    "",
                    "tasks",
                    BasicPublishOptions::default(),
                    b"hello",
                    BasicProperties::default(),
                )
                .await?;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
