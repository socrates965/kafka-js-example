// import the `Kafka` instance from the kafkajs library
const { Kafka } = require("kafkajs")
const log4js = require('log4js')
var os = require("os");

const logger = log4js.getLogger('')

// Catch uncaught exceptions
process.on('uncaughtException', function(error) {
    logger.error('Caught exception: ', error)
})

// Catch unhandled promise rejections
process.on('unhandledRejection', function(reason, p){
    logger.error('Unhandled Rejection at: Promise ', p, ' reason: ', reason)
})

let config = {
    hostname:null,
    broker:null,
    topic:null,
    group_id:null,
    username:null,
    password:null,
    log_level:"debug",
};
try {
    // kafka config
    config = {
        hostname:os.hostname(),
        broker:process.env.kafkaServer,
        topic:process.env.kafkaTopic,
        username:process.env.kafkaUser,
        password:process.env.kafkaPassword,
        log_level:process.env.logLevel,
    };

    // for debugging only
    // console.log("ENVIRONMENT VARIABLE: ", config);

    // Configure log4js
    log4js.configure({
        appenders: { 
            console: { type: 'stdout' }, // to write in console
            file: { type: 'file', filename: 'consumer.log' } // to write logs into file
        },
        categories: { default: { appenders: ['console'], level: config.log_level } }
    });
} catch (error) {
    console.error(error)
}

// initialize a new kafka client and initialize a producer from it
const kafka = new Kafka({
    clientId: config.hostname,
    brokers: [config.broker],

    ssl: {
        rejectUnauthorized: false,
        // ca: [fs.readFileSync('/my/custom/ca.crt', 'utf-8')],
        // key: fs.readFileSync('/my/custom/client-key.pem', 'utf-8'),
        // cert: fs.readFileSync('/my/custom/client-cert.pem', 'utf-8'),
    },

    // sasl mechanism
    sasl: {
        mechanism: 'plain', 
        username: config.username,
        password: config.password,
    },

    logCreator: logLevel => {
    return ({ namespace, level, label, log }) => {
        const { message, ...extra } = log
        logger.debug(`[${logLevel}] ${namespace} ${message} ${JSON.stringify(extra)}`)
    }
    }
})
const consumer = kafka.consumer({ groupId:config.group_id })

// we define an async function that consumes message 
const consume = async (config) => {
    try {
        // start consumer
        await consumer.connect()
        await consumer.subscribe({ topic: config.topic })

        // describe the group
        logger.debug(await consumer.describeGroup());

        // start consume message
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                // print message to log
                logger.debug(message.value.toString())
            },
        })
    } catch (error) {
        logger.error(error)
    }
}
consume(config).catch(console.error)
// module.exports = consume
