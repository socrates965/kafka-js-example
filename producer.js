// import the `Kafka` instance from the kafkajs library
const { Kafka } = require("kafkajs")
const log4js = require('log4js')
var os = require("os");

// sleep function for slow down loop
var sleepSetTimeout_ctrl;
function sleep(ms) {
    clearInterval(sleepSetTimeout_ctrl);
    return new Promise(resolve => sleepSetTimeout_ctrl = setTimeout(resolve, ms));
}

const logger = log4js.getLogger('')

let config = {
    hostname:null,
    broker:null,
    topic:null,
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
            file: { type: 'file', filename: 'producer.log' } // to write logs into file
        },
        categories: { default: { appenders: ['console'], level: config.log_level } }
    });
} catch (error) {
    console.error(error)
}



// Catch uncaught exceptions
process.on('uncaughtException', function(error) {
    logger.error('Caught exception: ', error)
})

// Catch unhandled promise rejections
process.on('unhandledRejection', function(reason, p){
    logger.error('Unhandled Rejection at: Promise ', p, ' reason: ', reason)
})

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
const producer = kafka.producer()

// we define an async function that writes a new message each second
const produce = async (config) => {
    try {
        logger.info(config);
        await producer.connect()
        let i = 0
        const topic = config.topic

        // after the produce has connected, we start an interval timer
        setInterval(async () => {
            // continues loop
            while (true) {
            // reduce the speed of loop
            await sleep(500)

            // send a message to the configured topic with the key and value formed from the current value of `i`
            logger.debug(await producer.send({
                topic,
                messages: [
                {
                    value: "this is message " + i,
                },
                ],
            }))

            // if the message is written successfully, log it and increment `i`
            logger.debug("Successfully produce: ", i)
            i++
            }
        }, 1000)
    } catch (error) {
        // if any error print to logs
        logger.error(error)
    }
}
produce(config).catch(console.error)
// module.exports = produce
