import { StreamInterface } from "./index";

async function runBaseSample(){
    const stream = new StreamInterface({
        "topics":{
            "process-topic":{
                "producesTo":["mqtt", "bullmq", "kafka"],
                "consumesFrom":["mqtt", "bullmq", "kafka"],
            },
        },
        'kafka': {
            'CLIENT_ID': 'flowbuild-test',
            'BROKER_HOST': 'localhost',
            'BROKER_PORT': '9092',
            'GROUP_CONSUMER_ID': 'flowbuild-test-consumer-group',
        },
        'bullmq': {
            'REDIS_HOST': 'localhost',
            'REDIS_PORT': '6379',
            'REDIS_PASSWORD': '',
            'REDIS_DB': 4,
        },
        'mqtt': {
            'MQTT_HOST': 'localhost',
            'MQTT_PORT': '1883',
            'MQTT_PROTOCOL': 'http',
            'MQTT_USERNAME': 'admin',
            'MQTT_PASSWORD': 'hivemq',
        }
    },);

    const consumerCallback = (topic: string, receivedMessage: string) => {
        console.log("** I'm a callback ");
        console.log({topic, receivedMessage});
    };

    await stream.connect(consumerCallback);

    await stream.produce(
        "process-topic", 
        {"mensagem": "This is an test"},
    );

    await stream.produce(
        "process-topic", 
        {"mensagem": "This is another test"},
    );
}
runBaseSample();