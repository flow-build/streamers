import { Consumer, Kafka, Producer, SASLOptions, KafkaConfig} from 'kafkajs';
import { LooseObject, } from '../types';
import { v4 as uuid, } from 'uuid';
import { EachMessagePayload, } from 'kafkajs';


export class KafkaStream {
    _client: Kafka;
    _producer: Producer;
    _consumer: Consumer;
  
    constructor(configs: LooseObject, client: any = Kafka) {
        console.log('[Kafka CONFIG] Starting configuration ...');
        const connectionConfig: KafkaConfig = {
            clientId: `${configs.CLIENT_ID}-${uuid()}`,
            brokers: [`${configs.BROKER_HOST}:${configs.BROKER_PORT}`],
        };
        if (configs.BROKER_KAFKA_MECHANISM && configs.BROKER_KAFKA_USERNAME && configs.BROKER_KAFKA_SECRET) {
            const sasl: SASLOptions = {
                mechanism: configs.mechanism,
                username: configs.username,
                password: configs.pswd,
            }; 
            connectionConfig.sasl = sasl;
            connectionConfig.ssl = !!sasl;
        }
        this._client = new client(connectionConfig);

        this._producer = this._client.producer();
        this._consumer = this._client.consumer({
            groupId: `${configs.GROUP_CONSUMER_ID}-consumer-group`,
        });
        console.log('[Kafka CONFIG] configurated.');
    }

    async connect(consumesFrom: Array<string>, producesTo: Array<string>, callback: any){
        if (consumesFrom.length) {
            console.log('[Kafka CONNECT] Consumer connecting ...');
            await this._consumer.connect();
            console.log('[Kafka CONNECT] Consumer connected ... setting consumers ...');
            await this.setConsumer(consumesFrom, callback);
        }
        if (producesTo.length) {
            console.log('[Kafka CONNECT] Producer connecting ...');
            await this._producer.connect();
            console.log('[Kafka CONNECT] Producer connected');
        }
    }

    async setConsumer(consumesFrom: Array<string>, callback: any){
        for (const topic of consumesFrom){
            console.log(`[Kafka CONSUMER] Creating consumer for "${topic}" ...`);
            await this._consumer.subscribe({
                topic: topic,
                fromBeginning: true,
            });
            console.log(`[Kafka CONSUMER] Consumer for "${topic}" created.`);
            await this._consumer.run({
                eachMessage: this.mountConsumerCallback(callback,),
            },);
            console.log(`[Kafka CONSUMER] Consumer for "${topic}" running. `);
        }
    }

    async produce({ topic, message }: { topic: string; message: LooseObject }){
        console.log('[Kafka PRODUCE] Publishing message ... ', { topic, message });
        try {
            await this._producer.send({
                topic,
                messages: [{ value: JSON.stringify(message) }],
            },);
            console.log('[Kafka PRODUCE] Message published.');
            return true;
        } catch (error) {
            console.error('[Kafka PRODUCE] Error publishing message.', error);
            return false;
        }
    }

    mountConsumerCallback(callback: any){
        return async ({ topic, partition, message, }: EachMessagePayload): Promise<void> => {
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
