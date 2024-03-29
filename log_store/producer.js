const { Kafka } = require("kafkajs");
const log_data = require("./system_logs.json");

createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_log_store_client",
      brokers: ["192.168.1.10:9092"]
    });

    const producer = kafka.producer();
    console.log("It will be connected to Producer..");

    await producer.connect();
    console.log("Connection successful.");

    let messages = log_data.map(item => {
      return {
        value: JSON.stringify(item),
        // partition: item.type == "system" ? 0 : 1
        partition: 0
      };
    });

    const message_result = await producer.send({
      topic: "LogStoreTopic",
      messages: messages
    });
    console.log("Sending is successful", JSON.stringify(message_result));
    
    await producer.disconnect();
  } catch (error) {
    console.log("Something went wrong!", error);
  } finally {
    process.exit(0);
  }
}