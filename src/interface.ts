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
        this.streams.forEach((stream) => {
            stream.configs = this._configs[stream.name];
            for (const [topic, tconfig] of Object.entries(this._configs.topics) as unknown as any) {
                if (tconfig.consumesFrom.includes(stream.name)) {
                    stream.topics.consumesFrom.push(topic);
                }
                if (tconfig.producesTo.includes(stream.name)) {
                    stream.topics.producesTo.push(topic);
                }
            }
        });
        for await (const stream of this.streams) {
            try {
                if (stream.topics?.consumesFrom.length || stream.topics?.producesTo.length) {
                    stream.stream = new stream.streamClass(stream.configs);
                    await stream.stream.connect(
                        stream.topics?.consumesFrom,
                        stream.topics?.producesTo,
                        callback,
                    );
                }
            } catch (error) {
                console.error(`[CONNECT] Error connect to ${stream.stream}.`, error);
            }
        }
    }

    async produce(topic: string, message: LooseObject) {
        for await (const stream of this.streams) {
            try {
                if (stream.topics.producesTo.includes(topic)) {
                    await stream.stream.produce({ topic, message });
                }
            } catch (error) {
                console.error('[PRODUCE] Error publishing message.', error);
            }
        }
    }

}
