const { Kafka } = require("kafkajs");

// node consumer.js Logs || Logs2
const topic_name = process.argv[2] || "Logs2";

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka(
        {
            clientId: "kafka_example_1",
            brokers: [ "192.168.1.10:9092" ]
        }
    );

    const consumer = kafka.consumer({
      groupId: "example_1_consumer_group_1"
    });
    console.log("Will be connected to Consumer..");

    await consumer.connect();
    console.log("Connected successfully.");

    // Consumer Subscribe..
    await consumer.subscribe(
        {
            topic: topic_name,
            fromBeginning: true
        }
    );

    await consumer.run(
        {
            eachMessage: async result => {
                console.log(
                `Incoming message ${result.message.value}, Partition => ${result.partition}, Topic => ${result.topic}`
                );
            }
        }
    );

  } catch (error) {
    console.log("Something went wrong.", error);
  }
}