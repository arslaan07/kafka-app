const { Kafka } = require('kafkajs')
const readline = require('readline')

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
})

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['192.168.29.52:9092'],
  })

  async function init() {
    const producer = kafka.producer()
    console.log('Connecting Producer...')
    await producer.connect()
    console.log('Producer connected successfully')

    rl.setPrompt('> ')
    rl.prompt()

    rl.on('line', async function(line) {
        const [riderName, location] = line.split(' ')
        console.log('Producing data...')
        await producer.send({
            topic: 'rider-updates',
            messages: [
                { partition: location.toLowerCase() === 'north' ? 0 : 1, key: "location-update", value: JSON.stringify({ name: riderName, loc: location }) }
            ]
        })
        console.log('Data produced successfully')
    }).on('close', async () => {
        console.log('Disconnecting producer...')
        await producer.disconnect()
        console.log('Producer disconnected successfully')
    })
  }

  init()