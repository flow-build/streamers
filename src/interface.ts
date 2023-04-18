import { KafkaStream, } from './streams/kafka';
import { BullmqStream, } from './streams/bullmq';
import { MqttStream } from './streams/smqtt';
import { RabbitMQStream } from './streams/rabbitmq';
import { LooseObject, } from './types';
import { Streamer } from './streamer';

export class StreamInterface {
    private _configs: LooseObject;
    streams: Streamer[] = [
        new Streamer('kafka', KafkaStream),
        new Streamer('bullmq', BullmqStream),
        new Streamer('mqtt', MqttStream),
        new Streamer('rabbitmq', RabbitMQStream),
    ];

    constructor(configs: LooseObject) {
        this._configs = configs;
    }

    async connect(callback?: any) {
        for await (const stream of this.streams) {
            if (stream.name in this._configs){
                stream.mountTopics(this._configs.topics);
                await stream.startStream(this._configs[stream.name], callback);
            }
        }
    }

    async produce(topic: string, message: LooseObject, options={}) {
        for await (const stream of this.streams) {
            await stream.produce({ topic, message, options });
        }
    }
}
