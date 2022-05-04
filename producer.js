import { Kafka } from 'kafkajs'
import { spawn } from 'child_process'

class KafkaProducer {
  static #instance = null;
  #admin = null;
  #producer = null;
  // #consumer = null;
  #isConnected = false;

  constructor() {
    const kafka = new Kafka({
      clientId: 'my-app',
      brokers: ['127.0.0.1:9092'],
    });
    this.#admin = kafka.admin();
    this.#producer = kafka.producer();
  }

  static getInstance() {
    if (!KafkaProducer.#instance) {
      KafkaProducer.#instance = new KafkaProducer();
    }
    return KafkaProducer.#instance;
  }

  get isConnected() {
    return this.#isConnected;
  }

  async connect() {
    try {
      await Promise.all([
        this.#producer.connect(),
        this.#admin.connect(),
      ]);
      this.#isConnected = true;
      return this.#producer;
    }
    catch (e) {
      console.error('async connect error:', e)
    }
  }

  async createTopics(topics) {
    try {
      return await this.#admin.createTopics(topics);
    }
    catch (e) {
      console.error('async createTopics error:', e);
    }
  }
  async send(topic, value, partition) {
    try {
      await this.#producer.send({
        topic,
        messages: [{
          value,
          partition
        }]
      })
      return this.#producer
    }
    catch (e) {
      console.error('async send error', e);
    }
  }

  get producer() {
    return this.#producer;
  }
  // get consumer() {
  //   return this.#consumer;
  // }
  // get admin() {
  //   return this.#admin;
  // }
}
//-----------------------------------------------------

// create a new producer instance
const kafka = KafkaProducer.getInstance();

// initialize admin and producer
await kafka.connect();
await kafka.createTopics({
  waitForLeaders: true,
  topics: [
    {
      topic: 'topic1',
      numPartitions: 2,
    },
    {
      topic: 'topic2',
      numPartitions: 2,
    },
    {
      topic: 'topic3',
      numPartitions: 2,
    },
    {
      topic: 'topic4',
      numPartitions: 2,
    },
  ]
})

// initialize consumer
const consumer = spawn('node', ['consumer.js'])
consumer.stdout.on('data', data => console.log(data.toString()))

// send test data from producer to kafka cluster every 5000 ms
while (true) {
  const sent = await Promise.all([
    kafka.send('topic1', 'hi topic1, partition0', 0),
    kafka.send('topic1', 'hi topic1, partition1', 1),
    kafka.send('topic1', 'hi topic1, partition0', 0),
    kafka.send('topic1', 'hi topic1, partition1', 1),
    kafka.send('topic2', 'hi topic2, partition1', 1),
    kafka.send('topic3', 'hi topic3, partition0', 0),
    kafka.send('topic3', 'hi topic3, partition0', 0),
    kafka.send('topic3', 'hi topic3, partition0', 0),
    kafka.send('topic3', 'hi topic3, partition1', 1),
    kafka.send('topic4', 'hi topic4, partition0', 0),
    kafka.send('topic4', 'hi topic4, partition0', 0),
    kafka.send('topic4', 'hi topic4, partition0', 0),
    kafka.send('topic4', 'hi topic4, partition0', 0),
    kafka.send('topic4', 'hi topic4, partition0', 0),
    kafka.send('topic4', 'hi topic4, partition1', 1),
  ])
  console.log(sent);
  console.log("----------------------------")
  await new Promise(resolve => setTimeout(resolve, 5000))
}

