import { Job, Queue, Worker, } from 'bullmq';
import { LooseObject, } from '../types';


export class BullmqStream {
    _connection: LooseObject;
    _flowbuildQueues: { [key: string]: Queue };
    _flowbuildWorkers: { [key: string]: Worker };
  
    constructor(configs: LooseObject) {
        console.log('[Bullmq CONFIG] Starting configuration ...');
        this._connection = {
            host: configs.REDIS_HOST,
            port: parseInt(configs.REDIS_PORT, 10),
            password: configs.REDIS_PASSWORD,
            db: configs.REDIS_DB,
        };
        this._flowbuildQueues = {};
        this._flowbuildWorkers = {};
        console.log('[Bullmq CONFIG] configurated.');
    }

    async connect(consumesFrom: Array<string>, producesTo: Array<string>, callback:any,) {
        if (consumesFrom.length) {
            console.log('[Bullmq CONNECT] Creating consumers ...',);
            await this.setConsumer(consumesFrom, callback,);
            console.log('[Bullmq CONNECT] Consumers created.',);
        }
        if (producesTo.length) {
            console.log('[Bullmq CONNECT] Creating producers ...',);
            await this.setProduces(producesTo,);
            console.log('[Bullmq CONNECT] Producers created.',);
        }
    }

    async setConsumer(topics: Array<string>, callback: any,){
        for await (const topic of topics){
            console.log(`[Bullmq CONSUMER] Creating consumer for "${topic}"...`);
            this._flowbuildWorkers[topic] = this.createWorker(
                topic, this.mountConsumerCallback(callback)
            );
            console.log(`[Bullmq CONSUMER] Consumer for "${topic}" created.`);
        }
    }

    async setProduces(topics: Array<string>,){
        for (const topic of topics) {
            console.log(`[Bullmq PRODUCER] Creating Producer for "${topic}" ...`);
            this._flowbuildQueues[topic] = this.createQueue(topic);
            console.log(`[Bullmq PRODUCER] Producer for "${topic}" created.`);
        }
    }

    async produce({ topic, message }: { topic: string; message: LooseObject }){
        console.log('[Bullmq PRODUCE] Publishing message ... ', { topic, message });
        const queue = this._flowbuildQueues[topic];
        if (queue) {
            try {
                await queue.add(
                    topic, 
                    JSON.stringify(message,), 
                    {
                        removeOnComplete: true, 
                        removeOnFail: false,
                        attempts: 3,
                        backoff: {
                            type: 'exponential',
                            delay: 1000,
                        },
                    },
                );
                console.log('[Bullmq PRODUCE] Message published.');
                return true;
            } catch (error) {
                console.error('[Bullmq PRODUCE] Error publishing message.', error);
                return false;
            }
        }
        return false;
    }
    
    createQueue(topic: string){
        return new Queue(topic, {connection: this._connection});
    }

    createWorker(topic: string, callback: any){
        return new Worker(
            topic, 
            this.mountConsumerCallback(callback,), 
            {connection: this._connection,},
        );
    }

    mountConsumerCallback(callback: any,){
        return async (job: Job,) => {
            const receivedMessage = job.data;
            console.log(`[Bullmq CONSUMER] Message received -> ${JSON.stringify(
                {
                    value: receivedMessage,
                },
            )}`);
            callback(job.name, receivedMessage);
        };
    }
}
