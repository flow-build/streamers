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

class RabbitMQStreamMock {
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

    beforeEach(async () => {
        jest.clearAllMocks();
        stream = new StreamInterface({
            "topics":{
                "process-topic":{
                    "producesTo":["bullmq", "kafka", 'mqtt', "rabbitmq"],
                    "consumesFrom":["bullmq", "kafka", 'mqtt', "rabbitmq"],
                },
                "process-dynamic-$":{
                    "producesTo":["bullmq", "kafka", 'mqtt', "rabbitmq"],
                    "consumesFrom":["bullmq", "kafka", 'mqtt', "rabbitmq"],
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
            },
            'rabbitmq': {
                'RABBITMQ_HOST': 'localhost:5672',
                'RABBITMQ_USERNAME': 'user',
                'RABBITMQ_PASSWORD': 'password',
                'RABBITMQ_QUEUE': 'flowbuild'
            }
        });
        stream.streams = [
            new Streamer('kafka', KafkaStreamMock),
            new Streamer('bullmq', BullmqStreamMock),
            new Streamer('mqtt', MqttStreamMock),
            new Streamer('rabbitmq', RabbitMQStreamMock),
        ];
        const consumerCallback = (topic: string, receivedMessage: string) => {};
        await stream.connect(consumerCallback);
        for await (const strm of stream.streams) {
            expect(strm.topics.producesTo).toEqual(["process-topic"]);
            expect(strm.topics.producesToDynamic).toEqual([/process-dynamic-.*/]);
            expect(strm.topics.consumesFrom).toEqual(["process-topic", "process-dynamic-$"]);
            expect(strm.broker.connect).toHaveBeenNthCalledWith(1,
                ["process-topic", "process-dynamic-$"], 
                ["process-topic"], 
                consumerCallback
            );
        }
    });

    it('Check if base case is working', async () => {
        await stream.produce(
            "process-topic", 
            {"mensagem": "This is an test"},
        );
        for await (const strm of stream.streams) {
            expect(strm.broker.produce).toHaveBeenLastCalledWith(
                {
                    "message": {"mensagem": "This is an test"}, 
                    "topic": "process-topic",
                    "options": {}
                }
            );
        }
    });

    it('Check if case with options is working', async () => {
        await stream.produce(
            "process-topic", 
            {"mensagem": "This is an test with options"},
            {"delay": 5000}
        );
        for await (const strm of stream.streams) {
            expect(strm.broker.produce).toHaveBeenLastCalledWith(
                {
                    "message": {"mensagem": "This is an test with options"}, 
                    "topic": "process-topic",
                    "options": {"delay": 5000}
                }
            );
        }
    });

    it('Check if case with dynamic topics is working', async () => {
        await stream.produce(
            "process-dynamic-topic", 
            {"mensagem": "This is an test for dynamic topics"}
        );
        for await (const strm of stream.streams) {
            expect(strm.broker.produce).toHaveBeenLastCalledWith(
                {
                    "message": {"mensagem": "This is an test for dynamic topics"}, 
                    "topic": "process-dynamic-topic",
                    "options": {}
                }
            );
        }
    });
});
