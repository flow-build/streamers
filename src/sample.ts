import { StreamInterface } from "./index";

async function runBaseSample(){
    const stream = new StreamInterface({
        "topics":{
            "process-topic":{
                "producesTo":["kafka", 'bullmq', 'mqtt', 'rabbitmq'],
                "consumesFrom":["kafka", 'bullmq', 'mqtt', 'rabbitmq'],
            },
            "process-topic-second":{
                "producesTo":["kafka", 'bullmq'],
                "consumesFrom":["kafka", 'bullmq'],
            },
            "trigger_event_$":{
                "producesTo":["kafka"],
                "consumesFrom":["kafka"],
            },
            "trigger_event_xpto":{
                "consumesFrom":["kafka"],
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
        },
        'rabbitmq': {
            'RABBITMQ_HOST': 'localhost:5672',
            'RABBITMQ_USERNAME': 'user',
            'RABBITMQ_PASSWORD': 'password',
            'RABBITMQ_QUEUE': 'flowbuild'
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
        "process-topic-second", 
        {"mensagem": "This is another test"},
    );

    await stream.produce(
        "trigger_event_xpto", 
        {"mensagem": "This is a test abount trigger event xpto"},
        {"delay": 5000}
    );

    /*await stream.produce(
        "process-states-topic", 
        {
            "actor_id": "093eaeaa-daea-11ed-afa1-0242ac120002",
            "process_data": {
                "process_id":"25513520-bd31-4d58-9f15-b33d3fdc1ee9", 
                "workflow_name": "PARENT", 
                "state": {
                    "node_id": "node_id",
                    "next_node_id": "next_node_id",
                    "result": {},
                    "status": "finished",
                    "error": {},
                    "time_elapsed": 0,
                }
            },
            "visibility":["common"]            
        },
    );*/

    /*await stream.produce(
        "orchestrator-start-process-topic", 
        {
            "actor": {
                "id": "0f891818-daea-11ed-afa1-0242ac120002",
                "roles": ["role"],
                "iat": 0
            },
            "workflow": { 
                "name": "PARENT"
            },
            "process_id":"25513520-bd31-4d58-9f15-b33d3fdc1ee9",           
        },
    );*/

    await stream.produce("orchestrator-result-topic",
        {
            result: {},
            workflow: { 
                "name": "PARENT"
            },
            process_id: "25513520-bd31-4d58-9f15-b33d3fdc1ee9",
            actor: {
                "id": "0f891818-daea-11ed-afa1-0242ac120002",
                "roles": ["role"],
                "iat": 0
            },
        }
    );
    
    /*await stream.produce("trigger-resolver-topic",{});
    await stream.produce("start-nodes-topic",{});
    await stream.produce("finish-nodes-topic",{});
    await stream.produce("http-nodes-topic",{});
    await stream.produce("form-request-nodes-topic",{});
    await stream.produce("flow-nodes-topic",{});
    await stream.produce("js-script-task-nodes-topic",{});
    await stream.produce("user-task-nodes-topic",{});
    await stream.produce("timer-nodes-topic",{});
    await stream.produce("system-task-nodes-topic",{});
    await stream.produce("event-nodes-topic",{});*/

}
runBaseSample();