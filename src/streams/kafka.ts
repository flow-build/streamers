import { Consumer, Kafka, Producer, } from 'kafkajs';
import { LooseObject, } from '../types';
import { v4 as uuid, } from 'uuid';
import { EachMessagePayload, } from 'kafkajs';


export class KafkaStream {
    _client: Kafka;
    _producer: Producer;
    _consumer: Consumer;
  
    constructor(configs: LooseObject, client: any = Kafka) {
        console.log('>> Starting Kafka config ...',);
        this._client = new client({
            clientId: `${configs.CLIENT_ID}-${uuid()}`,
            brokers: [`${configs.BROKER_HOST}:${configs.BROKER_PORT}`,],
        },);
  
        this._producer = this._client.producer();
        console.log('>> Kafka producer created ...',);
        this._consumer = this._client.consumer({
            groupId: `${configs.GROUP_CONSUMER_ID}-consumer-group`,
        },);
        console.log('>> Kafka consumer created ...',);
    }

    async connect(consumesFrom: Array<string>, producesTo: Array<string>, callback: any,){
        if (consumesFrom.length) {
            console.log('[CONNECT] Kafka consumer connecting ...',);
            await this._consumer.connect();
            console.log('[CONNECT] Kafka consumer connected ... setting consumers ...',);
            await this.setConsumer(consumesFrom, callback,);
        }
        if (producesTo.length) {
            console.log('[CONNECT] Kafka producer connecting ...',);
            await this._producer.connect();
            console.log('[CONNECT] Kafka producer connected',);
        }
    }

    async setConsumer(consumesFrom: Array<string>, callback: any,){
        for (const topic of consumesFrom){
            await this._consumer.subscribe({
                topic: topic,
                fromBeginning: true,
            },);
            console.log(`[CONSUMER] Kafka consumer for "${topic}" created ...`,);
            await this._consumer.run({
                eachMessage: this.mountConsumerCallback(callback,),
            },);
            console.log(`[CONSUMER] Kafka consumer for "${topic}" running `,);
        }
    }

    async produce({ topic, message, }: { topic: string; message: LooseObject },){
        console.log('[PRODUCE] Publishing message ... ', { topic, message, },);
        try {
            await this._producer.send({
                topic,
                messages: [{ value: JSON.stringify(message,), },],
            },);
            console.log('[PRODUCE] Message published.',);
            return true;
        } catch (error) {
            console.error('[PRODUCE] Error publishing message.', error,);
            return false;
        }
    }

    mountConsumerCallback(callback: any,){
        return async ({ topic, partition, message, }: EachMessagePayload,): Promise<void> => {
            const receivedMessage = message.value?.toString() || '';
            console.log(`[CONSUMER] Message received (Kafka) -> ${JSON.stringify(
                {
                    partition,
                    offset: message.offset,
                    value: receivedMessage,
                },
            )}`,);
            callback(topic, receivedMessage,);
        };
    }
}
