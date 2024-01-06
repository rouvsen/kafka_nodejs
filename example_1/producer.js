const { Kafka } = require("kafkajs");

const topic_name = process.argv[2] || "Logs2";
const partition = process.argv[3] || 0;

createProducer();

async function createProducer() {
    try {

        const kafka = new Kafka(
            {
                clientId: "kafka_example_1",
                brokers: [ "192.168.1.10:9092" ]
            }
        );

        const producer = kafka.producer();
        console.log("Will be connected to Producer..");

        await producer.connect();
        console.log("Connected successfully");

        const message_result = await producer.send(
            {
                topic: topic_name,
                messages: [
                    {
                        value: "This is test message..",
                        partition: partition
                    }
                ]
            }
        )
        console.log("Message sent successfully", JSON.stringify(message_result));
        await producer.disconnect();
    } catch(error) {
        console.log("Something went wrong", error);
    } finally {
        process.exit(0);
    }
}