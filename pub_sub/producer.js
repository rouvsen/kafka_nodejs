const { Kafka } = require("kafkajs");

createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_pub_sub_client",
      brokers: ["192.168.1.10:9092"]
    });

    const producer = kafka.producer();
    console.log("It will be connected to Producer..");

    await producer.connect();
    console.log("Connection successful.");

    const message_result = await producer.send({
      topic: "raw_video_topic",
      messages: [
        {
          value: "New Video Content",
          partition: 0
        }
      ]
    });
    console.log("Sending is successful", JSON.stringify(message_result));

    await producer.disconnect();
  } catch (error) {
    console.log("Something went wrong!", error);
  } finally {
    process.exit(0);
  }
}