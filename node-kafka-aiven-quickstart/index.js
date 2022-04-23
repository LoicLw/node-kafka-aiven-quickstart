const Kafka = require('node-rdkafka');
const yargs = require('yargs');

//Usage
//node index.js --host kafka-instance.aivencloud.com:13301  --key-path kafka-instance-service.key --cert-path kafka-instance-service.cert --ca-path kafka-instance-ca.pem
const argv = yargs
    .option('host', {
        description: 'Kafka host using host:port',
        type: 'string',
        required: true
    }).option('host', {
        description: 'InfluxDB host',
        type: 'string',
        required: true
    }).option('key-path', {
        description: 'Path to service.key',
        type: 'string',
        required: true
    }).option('cert-path', {
        description: 'Path to service.cert',
        type: 'string',
        required: true
    }).option('ca-path', {
        description: 'Path to ca.pem',
        type: 'string',
        required: true
    })
    .argv;

//Making sure node-rdkafka was compiled with SSL support. 
//Make sure OpenSSL is installed and accessible by the compiler
if (!Kafka.features.includes('ssl')) {
    console.log('We require node-rdkafka to be compiled with ssl support.');
    process.exit(1)
}

//Topic has to be created on Kafka instance
TOPIC = 'iotSensors';

let numEvents = 50;

//Generating random IoT data
IoTData = []
for (let idx = 0; idx < numEvents; ++idx) {
    IoTData.push({
        "sensorId": "Launchroom-" + [...Array(20)].map(() => 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'[Math.floor(Math.random()*62)]).join(''),
        "status": "ACTIVE",
        "latitude": 37.5638998,
        "longitude": -116.8511523,
        "temperature": 20+Math.floor(Math.random()*10),
        "pressure": Math.floor(Math.random()*100),
        "timestamp": new Date().toISOString()
    })
}

//Connecting to Kafka instance and producing messages 
function kafkaProduce(host, keyPath, certPath, caPath) {
    const producer = new Kafka.Producer({
        'metadata.broker.list': host,
        'security.protocol': 'ssl',
        'ssl.key.location': keyPath,
        'ssl.certificate.location': certPath,
        'ssl.ca.location': caPath,
        'dr_cb': true
    });

    producer.connect();
    producer.on('ready', function () {
        try {
            for (let idx = 1; idx < numEvents +1; ++idx) {
                data = IoTData.pop()
                producer.produce(
                    TOPIC,  // topic to send the message to
                    null,  // partition, null for librdkafka default partitioner
                    new Buffer.from(JSON.stringify(data).toString()),  // value
                    'sensorData',  // (optional) key
                    Date.now()  // (optional) timestamp
                );
                producer.flush(2000);
                console.info("Sensor data: ", data)
                console.log('Message ', idx ,' successfully streamed to '+ argv.host);
            }
        } catch (err) {
            console.log('Failed to send message', err);
        }
        producer.disconnect();
    });
}

kafkaProduce(argv.host, argv.keyPath, argv.certPath, argv.caPath);