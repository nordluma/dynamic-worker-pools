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

    // channel
    //     .queue_bind(
    //         "tasks",
    //         "tasks",
    //         "",
    //         QueueBindOptions::default(),
    //         FieldTable::default(),
    //     )
    //     .await?;

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

    Ok(())
}
