const { Kafka } = require("kafkajs");

createTopic();

async function createTopic() {
  try {
    // Admin Stuff..
    const kafka = new Kafka({
      clientId: "kafka_pub_sub_client",
      brokers: ["192.168.1.10:9092"]
    });

    const admin = kafka.admin();
    console.log("Kafka will be connected to Broker..."); 

    await admin.connect();
    console.log("Kafka connected to Broker successfully, Topic will be created.."); 

    await admin.createTopics({
      topics: [
        {
          topic: "raw_video_topic",
          numPartitions: 1
        }
      ]
    });
    console.log("Topic created successfully..."); 

    await admin.disconnect();
  } catch (error) {
    console.log("Something went wrong!", error);
  } finally {
    process.exit(0);
  }
}