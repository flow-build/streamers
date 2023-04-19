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
        await stream.connect();
        expect(stream._client.producer).toHaveBeenCalled();
        expect(stream._client.consumer).toHaveBeenCalled();
        expect(stream._consumer.connect).toHaveBeenCalled();
        expect(stream._producer.connect).toHaveBeenCalled();

        for (const topic of ["process-topic", "process-dynamic-$"]){
            await stream.setConsumer(topic,consumerCallback);
            await stream.runConsumer();
        }
        expect(stream._consumer.subscribe).toHaveBeenCalled();
        expect(stream._consumer.run).toHaveBeenCalled();

        await stream.produce({
            "topic":"process-topic", 
            "message":{"mensagem": "This is an test"},
        });
        expect(stream._producer.send).toHaveBeenCalled();

        await stream.produce({
            "topic":"process-dynamic-topic", 
            "message":{"mensagem": "This is an test for dynamic"},
        });
        expect(stream._producer.send).toHaveBeenCalled();
    });

});

