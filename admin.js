const { Kafka } = require('kafkajs')


const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['192.168.29.52:9092'],
  })

  async function init() {
    const admin = kafka.admin()
    console.log('Admin connecting...')
    await admin.connect()
    console.log('Admin connection success...')
    
    console.log('Creating topic[]...')
    await admin.createTopics({
        topics: [{
            topic: 'rider-updates',
            numPartitions: 2
        }]
    })
    console.log('Topic Created')

    console.log('Disconnecting admin...')
    admin.disconnect()
  }

  init()