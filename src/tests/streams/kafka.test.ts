/* eslint-disable no-unused-vars */
/* eslint-disable no-undef */
import { KafkaStream } from '../../streams/kafka';

class KafkaMock {
    producer = jest.fn(() => {
        return {
            connect: jest.fn(() => {}),
            send: jest.fn((topic, messages) => {}),
        };
    });
    consumer = jest.fn(({groupId: string}) => {
        return {
            connect: jest.fn(() => {}),
            subscribe: jest.fn((topic, fromBeginning) => {}),
            run: jest.fn(({eachMessage}) => {}),
        };
    });
}

describe('Kafka Stream suite test', () => {
    let stream: KafkaStream;

    beforeEach(() => {
        jest.clearAllMocks();
    });

    it('Check if base case is working', async () => {
        const configs = {
            'CLIENT_ID': 'flowbuild-test',
            'BROKER_HOST': 'localhost',
            'BROKER_PORT': '9092',
            'GROUP_CONSUMER_ID': 'flowbuild-test-consumer-group',
        };
        const consumerCallback = (topic: string, receivedMessage: string,) => {};
        
        KafkaStream.createClient = jest.fn().mockReturnValue(new KafkaMock());
        stream = new KafkaStream(configs);
        expect(stream._client.producer).toHaveBeenCalledTimes(1);
        expect(stream._client.consumer).toHaveBeenCalledTimes(1);

        await stream.connect(
            ["process-topic", "process-dynamic-$"], 
            ["process-topic", "process-dynamic-$"], 
            consumerCallback
        );
        expect(stream._producer.connect).toHaveBeenCalledTimes(1);
        expect(stream._consumer.connect).toHaveBeenCalledTimes(1);
        expect(stream._consumer.subscribe).toHaveBeenCalledTimes(2);
        expect(stream._consumer.run).toHaveBeenCalledTimes(1);

        await stream.produce({
            "topic":"process-topic", 
            "message":{"mensagem": "This is an test"},
        });
        expect(stream._producer.send).toHaveBeenCalledTimes(1);

        await stream.produce({
            "topic":"process-dynamic-topic", 
            "message":{"mensagem": "This is an test for dynamic"},
        });
        expect(stream._producer.send).toHaveBeenCalledTimes(2);
    });

});

