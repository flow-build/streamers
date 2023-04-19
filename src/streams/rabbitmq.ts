import { LooseObject, ProduceParam } from '../types';
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

    async connect(){
        const url = `amqp://${this._configs.username}:${this._configs.password}@${this._configs.hostname}`;
        this._client = await RabbitMQStream._connectClient(url);
    }

    static async _connectClient(url: string){
        return await amqplib.connect(url);
    }

    async setConsumer(topic: string, callback: any, dynamicKey = "$"){
        const channelCreated = await this._createChannel(topic, dynamicKey);
        await channelCreated.consume(topic, this.mountConsumerCallback(topic, channelCreated, callback));
    }

    async runConsumer(){
    }

    // eslint-disable-next-line no-unused-vars
    async _createChannel(topic: string, dynamicKey = "$"){
        const channelCreated = await this._client.createChannel();
        await channelCreated.assertQueue(topic, {
            durable: true,
            arguments: {
                'x-queue-type': 'classic',
            },
        });
        return channelCreated;
    }

    async produce({ topic, message }: ProduceParam){
        const channelCreated = await this._createChannel(topic);
        await channelCreated.sendToQueue(
            topic, 
            // eslint-disable-next-line no-undef
            Buffer.from(JSON.stringify(message))
        );
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
