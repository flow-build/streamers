import { KafkaStream, } from './streams/kafka';
import { BullmqStream, } from './streams/bullmq';
import { MqttStream } from './streams/smqtt';
import { RabbitMQStream } from './streams/rabbitmq';
import { LooseObject, } from './types';
import { Streamer } from './streamer';


export class StreamInterface {
    private _configs: LooseObject;
    streams: Streamer[];

    constructor(configs: LooseObject) {
        this._configs = configs;
        this.streams = StreamInterface.mountStreamers();
    }

    static mountStreamers(){
        return [
            new Streamer('kafka', KafkaStream),
            new Streamer('bullmq', BullmqStream),
            new Streamer('mqtt', MqttStream),
            new Streamer('rabbitmq', RabbitMQStream),
        ];
    }

    async connect(callback?: any) {
        for await (const stream of this.streams) {
            if (stream.name in this._configs){
                await stream.startStream(
                    this._configs.topics,
                    this._configs[stream.name], 
                    callback
                );
            }
        }
    }

    async produce(topic: string, message: LooseObject, options={}) {
        for await (const stream of this.streams) {
            await stream.produce({ topic, message, options });
        }
    }
}
