/* eslint-disable no-unused-vars */
/* eslint-disable no-undef */
import { Queue } from 'bullmq';
import { BullmqStream } from '../../streams/bullmq';

class WorkerMock{}
class QueueMock{}

describe('Bullmq Stream suite test', () => {
    let stream: BullmqStream;

    it('Check if base case is working', async () => {
        const consumerCallback = (topic: string, receivedMessage: string,) => {};

        stream = new BullmqStream({
            'REDIS_HOST': 'localhost',
            'REDIS_PORT': '6379',
            'REDIS_PASSWORD': '',
            'REDIS_DB': 4,
        });
        stream.connect();
        stream.createWorker = jest.fn().mockReturnValue(new WorkerMock());
        stream.createQueue = jest.fn().mockReturnValue(new QueueMock());
        for (const topic of ["process-topic", "process-dynamic-$"]){
            await stream.setConsumer(topic,consumerCallback);
            await stream.runConsumer();
        }
        const queue = await stream.getOrCreateFlowbuildQueue("process-topic");
        queue.add = jest.fn().mockReturnValue({});
        let result = await stream.produce({
            "topic":"process-topic", 
            "message":{"mensagem": "This is an test"},
        });
    
    });
});
