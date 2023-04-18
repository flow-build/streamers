import { LooseObject, ProduceParam, } from '../types';
import { v4 as uuid, } from 'uuid';
import * as mqtt from "mqtt";


export class MqttStream {
    _configs: LooseObject;
    _client: any;

    constructor(configs: LooseObject) {
        console.log('[Mqtt CONFIG] Starting configuration ...');
        this._configs = {
            hostname: configs.MQTT_HOST,
            port: configs.MQTT_PORT,
            protocol: configs.MQTT_PROTOCOL || "ws",
            path: configs.MQTT_PATH || "/mqtt",
            clientId: `configs.CLIENT_ID_${uuid()}`,
            username: configs.MQTT_USERNAME,
            password: configs.MQTT_PASSWORD
        };
        console.log('[Mqtt CONFIG] configurated.');
    }

    async connect(consumesFrom: Array<string>, producesTo: Array<string>, callback: any){
        console.log('[Mqtt CONNECT] Starting connection ...'); 
        this._client = mqtt.connect(this._configs);
        console.log('[Mqtt CONNECT] Connected.');
        this.setConsumer(consumesFrom, callback);
    }

    async setConsumer(consumesFrom: Array<string>, callback: any){
        for (const topic of consumesFrom){
            await this._client.subscribe(topic);
            console.log(`[Mqtt CONSUMER] Consumer for "${topic}" created.`);
        }
        await this._client.on("message", this.mountConsumerCallback(callback));
    }

    async produce({ topic, message, options }: ProduceParam){
        console.log('[Mqtt PRODUCE] Publishing message ... ', { topic, message });
        if (!options) {
            options = {};
        }
        if(options?.delay){
            await setTimeout(options?.delay);
        }
        try {
            await this._client.publish(
                topic,
                JSON.stringify(message),
            );
            console.log('[Mqtt PRODUCE] Message published.');
            return true;
        } catch (error) {
            console.error('[Mqtt PRODUCE] Error publishing message.', error);
            return false;
        }
    }

    async addConsumer(topic: string){
        console.log(`[Mqtt CONSUMER] Adding new consumer to ${topic} ... `);
    }

    mountConsumerCallback(callback: any){
        return async (topicSent: any, messageSentBuffer: any) => {
            console.log(`[Mqtt CONSUMER] Message received ${topicSent} -> ${JSON.stringify(
                {
                    value: messageSentBuffer.toString('utf-8'),
                },
            )}`);
            callback(topicSent, JSON.parse(messageSentBuffer.toString('utf-8')));
        };
    }
}
