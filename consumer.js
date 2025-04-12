const { Kafka } = require('kafkajs')
const group = process.argv[2]

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['192.168.29.52:9092'],
  }) 

  async function init() {
    const consumer = kafka.consumer({ groupId: group })
    console.log('Consumer connecting...')

    await consumer.connect()
    console.log('Consumer connected successfully')

    console.log('Consumer subscribing to topics...')

    await consumer.subscribe({ topics: ['rider-updates'], fromBeginning: true })
    console.log('Consumer subscribed successfully')

    console.log('Consumer listening ...')

    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            console.log(
                `[${group}: ${topic}]: PART: ${partition}:`, 
                message.value.toString() // Parse the actual message value
            )
        },
    })

    // await consumer.disconnect()

    // console.log('Consumer disconnected successfully')

  }

  init()