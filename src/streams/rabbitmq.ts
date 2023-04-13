import { LooseObject } from '../types';
import * as amqplib from "amqplib";

export class RabbitMQStream {
    _configs: LooseObject;
    _client: any;

    constructor(configs: LooseObject) {
        this._configs = {
            hostname: configs.RABBITMQ_HOST,
            username: configs.RABBITMQ_USERNAME,
            password: configs.RABBITMQ_PASSWORD,
        };
    }

    async connect(consumesFrom: Array<string>, producesTo: Array<string>, callback: any){
        console.log('[RabbitMQ CONNECT] Starting connection ...'); 
        const url = `amqp://${this._configs.username}:${this._configs.password}@${this._configs.hostname}`;
        this._client = await amqplib.connect(url);
        console.log('[RabbitMQ CONNECT] Connected.');
        await this.setConsumer(consumesFrom, callback);
    }

    async createChannel(topic: string){
        const channelCreated = await this._client.createChannel();
        await channelCreated.assertQueue(topic, {
            durable: true,
            arguments: {
                'x-queue-type': 'classic',
            },
        });
        return channelCreated;
    }

    async setConsumer(consumesFrom: Array<string>, callback: any){
        for (const topic of consumesFrom){
            console.log(`[RabbitMQ CONSUMER] Creating channel for "${topic}"`);
            const channelCreated = await this.createChannel(topic);
            await channelCreated.consume(topic, this.mountConsumerCallback(topic, channelCreated, callback));
            console.log(`[RabbitMQ CONSUMER] Channel for "${topic}" created and running`);
        }
    }

    async produce({ topic, message, }: { topic: string; message: LooseObject }){
        console.log('[RabbitMQ PRODUCE] Publishing message ... ', { topic, message });
        try {
            const channelCreated = await this.createChannel(topic);
            await channelCreated.sendToQueue(
                topic, 
                // eslint-disable-next-line no-undef
                Buffer.from(JSON.stringify(message))
            );
            console.log('[RabbitMQ PRODUCE] Message published.');
            return true;
        } catch (error) {
            console.error('[RabbitMQ PRODUCE] Error publishing message.', error);
            return false;
        }
    }

    mountConsumerCallback(topic: string, channel:any, callback: any){
        return async (msg: any) => {
            console.log(`[RabbitMQ CONSUMER] Message received ${topic} -> ${JSON.stringify(
                {
                    value: msg.content.toString('utf-8'),
                },
            )}`);
            callback(topic, JSON.parse(msg.content.toString('utf-8')));
            channel.ack(msg);
        };
    }
}
