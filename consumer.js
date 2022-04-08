import { Kafka } from 'kafkajs'

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['127.0.0.1:9092'],
});


const consumer = kafka.consumer({ groupId: 'test-group' });
await consumer.connect()

await Promise.all([
  consumer.subscribe({ topic: 'topic1', fromBeginning: true }),
  consumer.subscribe({ topic: 'topic2', fromBeginning: true }),
  consumer.subscribe({ topic: 'topic3', fromBeginning: true }),
  consumer.subscribe({ topic: 'topic4', fromBeginning: true }),
])

await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({
      topic,
      partition,
      value: message.value.toString(),
    })
  },
})
