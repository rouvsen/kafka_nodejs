const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_log_store_client",
      brokers: ["192.168.1.10:9092"]
    });

    const consumer = kafka.consumer({
      groupId: "log_store_consumer_group"
    });
    console.log("It will be connected to Consumer..");

    await consumer.connect();
    console.log("Connection successful.");

    // Consumer Subscribe..
    await consumer.subscribe({
      topic: "LogStoreTopic",
      fromBeginning: 
    });

    // await consumer.assign([
    //   { 
    //     topic: "LogStoreTopic",
    //     partition: 1
    //   }
    // ]);

    // await consumer.seek({
    //   topic: "LogStoreTopic",
    //   partition: 1,
    //   offset: 10
    // });

    await consumer.run({
      eachMessage: async result => {
        console.log(
          `Incoming message ${result.message.value}, Partition => ${result.partition}`
        );
      }
    });
  } catch (error) {
    console.log("Something went wrong!", error);
  }
}