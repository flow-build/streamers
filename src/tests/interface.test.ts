/* eslint-disable no-unused-vars */
/* eslint-disable no-undef */
import { StreamInterface } from '../index';
import { Streamer } from '../streamer';
import { LooseObject } from '../types';

class KafkaStreamMock {
    connect = jest.fn((
        consumesFrom: Array<string>, 
        producesTo: Array<string>, 
        callback: any,) => {});
    produce = jest.fn((
        { topic, message, }: { topic: string; message: LooseObject }
    ) => {});
}

class BullmqStreamMock {
    connect = jest.fn((
        consumesFrom: Array<string>, 
        producesTo: Array<string>, 
        callback: any,) => {});
    produce = jest.fn((
        { topic, message, }: { topic: string; message: LooseObject }
    ) => {});
}

class MqttStreamMock {
    connect = jest.fn((
        consumesFrom: Array<string>, 
        producesTo: Array<string>, 
        callback: any,) => {});
    produce = jest.fn((
        { topic, message, }: { topic: string; message: LooseObject }
    ) => {});
}

describe('Stream Interface suite test', () => {
    let stream: StreamInterface;

    beforeEach(() => {
        jest.clearAllMocks();
    });

    it('Check if base case is working', async () => {

        stream = new StreamInterface({
            "topics":{
                "process-topic":{
                    "producesTo":["bullmq", "kafka", 'mqtt'],
                    "consumesFrom":["bullmq", "kafka", 'mqtt'],
                },
            },
            'kafka': {
                'CLIENT_ID': 'flowbuild-test',
                'BROKER_HOST': 'localhost',
                'BROKER_PORT': '9092',
                'GROUP_CONSUMER_ID': 'flowbuild-test-consumer-group',
            },
            'bullmq': {
                'REDIS_HOST': 'localhost',
                'REDIS_PORT': '6379',
                'REDIS_PASSWORD': '',
                'REDIS_DB': 4,
            },
            'mqtt': {
                'MQTT_HOST': 'localhost',
                'MQTT_PORT': '1883',
                'MQTT_PROTOCOL': 'http',
                'MQTT_USERNAME': 'admin',
                'MQTT_PASSWORD': 'hivemq',
            }
        },);
        stream.streams = [
            new Streamer('kafka', KafkaStreamMock),
            new Streamer('bullmq', BullmqStreamMock),
            new Streamer('mqtt', MqttStreamMock),
        ];
        const consumerCallback = (topic: string, receivedMessage: string) => {};
        await stream.connect(consumerCallback);
        for await (const strm of stream.streams) {
            expect(strm.topics.producesTo).toEqual(["process-topic"]);
            expect(strm.topics.consumesFrom).toEqual(["process-topic"]);
            expect(strm.stream.connect).toHaveBeenNthCalledWith(1,
                ["process-topic"], 
                ["process-topic"], 
                consumerCallback
            );
        }
    
        await stream.produce(
            "process-topic", 
            {"mensagem": "This is an test"},
        );
        for await (const strm of stream.streams) {
            expect(strm.stream.produce).toHaveBeenNthCalledWith(1,
                {
                    "message": {"mensagem": "This is an test"}, 
                    "topic": "process-topic"
                }
            );
        }
    },);
},);
