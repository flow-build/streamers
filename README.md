<h1 align="center" style="border-bottom: none;">FlowBuild Streamers</h1>
<h3 align="center">This library provides functionality for writing and consuming events across multiple brokers</h3>

![Coverage lines](./coverage/badge-lines.svg)
![Coverage branches](./coverage/badge-branches.svg)
![Coverage functions](./coverage/badge-functions.svg)
![Coverage statements](./coverage/badge-statements.svg)

## Dependencies:

It is necessary to have a Message Broker running in order to use the Stream Interface. The available Brokers are:
```
kafka
bullmq
```

## Install:

```bash
npm i @flowbuild/streamers
```

## Configuration:

The required configuration object has a structure similar to:
```json
{
    "topics":{
        "event-topic":{
            "producesTo":["bullmq", "kafka",],
            "consumesFrom":["bullmq", "kafka",],
        },
    },
    "kafka": {
        "CLIENT_ID": "my-kafka-id",
        "BROKER_HOST": "localhost",
        "BROKER_PORT": "9092",
        "GROUP_CONSUMER_ID": "my-consumer-group",
    },
    "bullmq": {
        "REDIS_HOST": "localhost",
        "REDIS_PORT": "6379",
        "REDIS_PASSWORD": "",
        "REDIS_DB": 4,
    },
}
```
In *topics* you must put the name of the events and a relation of Consumption and Production listing the brokers that will be used.

For each broker you want to use, you must put the necessary configuration in the respective configuration key

## Example:

```typescript
const stream = new StreamInterface({
    "topics":{
        "event-topic":{
            "producesTo":["bullmq", "kafka",],
            "consumesFrom":["bullmq", "kafka",],
        },
    },
    "kafka": {
        "CLIENT_ID": "my-kafka-id",
        "BROKER_HOST": "localhost",
        "BROKER_PORT": "9092",
        "GROUP_CONSUMER_ID": "my-consumer-group",
    },
    "bullmq": {
        "REDIS_HOST": "localhost",
        "REDIS_PORT": "6379",
        "REDIS_PASSWORD": "",
        "REDIS_DB": 4,
    },
},);

const consumerCallback = (topic: string, receivedMessage: string,) => {
    console.log({topic, receivedMessage,},);
};

await stream.connect(consumerCallback,);
await stream.produce(
    "event-topic", 
    {"mensagem": "This is an test",},
);

await stream.produce(
    "event-topic", 
    {"mensagem": "This is another test",},
);
```
