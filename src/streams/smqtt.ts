import { LooseObject, ProduceParam } from '../types';
import { v4 as uuid } from 'uuid';
import * as mqtt from "mqtt";


export class MqttStream {
    _configs: LooseObject;
    _client: any;
    _callback: any;

    constructor(configs: LooseObject) {
        this._configs = {
            hostname: configs.MQTT_HOST,
            port: configs.MQTT_PORT,
            protocol: configs.MQTT_PROTOCOL || "ws",
            path: configs.MQTT_PATH || "/mqtt",
            clientId: `${configs.CLIENT_ID}_${uuid()}`,
            username: configs.MQTT_USERNAME,
            password: configs.MQTT_PASSWORD
        };
    }

    async connect(){
        this._client = await MqttStream._connectClient(this._configs);
    }

    static async _connectClient(configs: LooseObject){
        return await mqtt.connect(configs);
    }

    // eslint-disable-next-line no-unused-vars
    async setConsumer(topic: string, callback: any, dynamicKey = "$"){
        this._callback = this.mountConsumerCallback(callback);
        await this._client.subscribe(topic);
    }

    async runConsumer(){
        await this._client.on("message", this._callback);
    }

    async produce({ topic, message }: ProduceParam){
        await this._client.publish(
            topic, JSON.stringify(message),
        );
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
