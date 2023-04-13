/* eslint-disable no-unused-vars */
/* eslint-disable no-undef */
import { RabbitMQStream } from '../../streams/rabbitmq';

class RabbitMQMock {
    createChannel = jest.fn((topic) => {
        return {
            assertQueue: jest.fn(({ topic, config }) => {}),
            consume: jest.fn(({ topic, callback }) => {}),
            sendToQueue: jest.fn(({ topic, message }) => {}),
        };
    });
}

jest.mock("amqplib", () => {
    return {
        connect(url: string) { return new RabbitMQMock(); }
    };
});

describe('RabbitMQ Stream suite test', () => {
    let stream: RabbitMQStream;

    it('Check if base case is working', async () => {
        const consumerCallback = (topic: string, receivedMessage: string) => {};
        stream = new RabbitMQStream({
            'RABBITMQ_HOST': 'localhost:5672',
            'RABBITMQ_USERNAME': 'user',
            'RABBITMQ_PASSWORD': 'password',
            'RABBITMQ_QUEUE': 'flowbuild'
        });
        await stream.connect(
            ["process-topic"], 
            ["process-topic"], 
            consumerCallback
        );

        let result = await stream.produce({
            "topic":"process-topic", 
            "message":{"mensagem": "This is an test"},
        });
        expect(result).toEqual(true);

        stream.createChannel = jest.fn(() => { throw new Error("My Error"); });
        result = await stream.produce({
            "topic":"process-topic", 
            "message":{"mensagem": "This is an test"},
        });
        expect(result).toEqual(false);
    
    },);
},);
