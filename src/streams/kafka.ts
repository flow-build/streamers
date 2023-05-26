import { Consumer, Kafka, Producer, SASLOptions, KafkaConfig } from 'kafkajs';
import { LooseObject, ProduceParam, } from '../types';
import { v4 as uuid, } from 'uuid';
import { EachMessagePayload, } from 'kafkajs';


export class KafkaStream {
    _client: any;
    _producer: Producer;
    _consumer: Consumer;
    _callback: any;

    constructor(configs: LooseObject) {
        this._client = KafkaStream.createClient(configs);
        this._producer = this._client.producer();
        this._consumer = this._client.consumer({
            groupId: `${configs.GROUP_CONSUMER_ID}-consumer-group`,
        });
    }

    async connect() {
        await this._consumer.connect();
        await this._producer.connect();
    }

    static createClient(configs: LooseObject) {
        const connectionConfig: KafkaConfig = {
            clientId: `${configs.CLIENT_ID}-${uuid()}`,
            brokers: [`${configs.BROKER_HOST}:${configs.BROKER_PORT}`],
        };
        if (configs.BROKER_KAFKA_MECHANISM && configs.BROKER_KAFKA_USERNAME && configs.BROKER_KAFKA_SECRET) {
            const sasl: SASLOptions = {
                mechanism: configs.BROKER_KAFKA_MECHANISM,
                username: configs.BROKER_KAFKA_USERNAME,
                password: configs.BROKER_KAFKA_SECRET,
            };
            connectionConfig.sasl = sasl;
            connectionConfig.ssl = !!sasl;
        }
        return new Kafka(connectionConfig);
    }

    async setConsumer(topic: string, callback: any, dynamicKey = "$") {
        this._callback = callback;
        let cleanTopic: string | RegExp = topic;
        if (topic.indexOf(dynamicKey) != -1) {
            cleanTopic = new RegExp(topic.replace(dynamicKey, ".*"));
        }
        await this._consumer.subscribe({
            topic: cleanTopic,
            fromBeginning: true,
        });
    }

    async runConsumer() {
        await this._consumer.run({
            eachMessage: this.mountConsumerCallback(this._callback),
        });
    }

    async produce({ topic, message }: ProduceParam) {
        const sendParams = {
            topic,
            messages: [{ value: JSON.stringify(message) }],
        };
        await this._producer.send(sendParams);
    }

    mountConsumerCallback(callback: any) {
        return async ({ topic, partition, message }: EachMessagePayload): Promise<void> => {
            const receivedMessage = message.value?.toString() || '';
            console.log(`[Kafka CONSUMER] Message received -> ${JSON.stringify(
                {
                    partition,
                    offset: message.offset,
                    value: receivedMessage,
                },
            )}`);
            callback(topic, receivedMessage);
        };
    }
}
