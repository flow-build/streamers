import { LooseObject, } from '../types';
import { v4 as uuid, } from 'uuid';
import * as mqtt from "mqtt";


export class MqttStream {
    _configs: LooseObject;
    _client: any;

    constructor(configs: LooseObject) {
        this._configs = {
            hostname: configs.MQTT_HOST,
            port: configs.MQTT_PORT,
            protocol: configs.MQTT_PROTOCOL || "ws",
            path: configs.MQTT_PATH || "/mqtt",
            clientId: `configs.CLIENT_ID_${uuid()}`,
            username: configs.MQTT_USERNAME,
            password: configs.MQTT_PASSWORD
        };
    }

    async connect(consumesFrom: Array<string>, producesTo: Array<string>, callback: any){
        console.log('[CONNECT] Starting MQTT connection ...'); 
        this._client = mqtt.connect(this._configs);
        this.setConsumer(consumesFrom, callback);
        console.log('[CONNECT] MQTT connected ...');
    }

    async setConsumer(consumesFrom: Array<string>, callback: any){
        for (const topic of consumesFrom){
            await this._client.subscribe(topic);
            console.log(`[CONSUMER] MQTT consumer for "${topic}" created and running`);
        }
        await this._client.on("message", this.mountConsumerCallback(callback));
    }

    async produce({ topic, message, }: { topic: string; message: LooseObject }){
        console.log('[PRODUCE] Publishing message ... ', { topic, message });
        try {
            await this._client.publish(
                topic,
                JSON.stringify(message),
            );
            console.log('[PRODUCE] Message published.');
            return true;
        } catch (error) {
            console.error('[PRODUCE] Error publishing message.', error);
            return false;
        }
    }

    mountConsumerCallback(callback: any){
        return async (topicSent: any, messageSentBuffer: any) => {
            console.log(`[CONSUMER] Message received (MQTT) ${topicSent} -> ${JSON.stringify(
                {
                    value: messageSentBuffer.toString('utf-8'),
                },
            )}`);
            callback(topicSent, JSON.parse(messageSentBuffer.toString('utf-8')));
        };
    }
}
