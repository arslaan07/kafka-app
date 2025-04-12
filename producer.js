const { Kafka } = require('kafkajs')


const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['192.168.29.52:9092'],
  })

  async function init() {
    const producer = kafka.producer()
    console.log('Connecting Producer...')
    await producer.connect()
    console.log('Producer connected successfully')

    console.log('Producing data...')
    await producer.send({
        topic: 'rider-updates',
        messages: [
            { partition: 0, key: "location-update", value: JSON.stringify({ name: 'Tony Stark', loc: 'SOUTH' }) }
        ]
    })
    console.log('Data produced successfully')

    console.log('Disconnecting producer...')
    await producer.disconnect()
    console.log('Producer disconnected successfully')
  }

  init()