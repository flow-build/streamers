import { Job, Queue, Worker, } from 'bullmq';
import { LooseObject, } from '../types';


export class BullmqStream {
    _connection: LooseObject;
    _flowbuildQueues: { [key: string]: Queue };
    _flowbuildWorkers: { [key: string]: Worker };
  
    constructor(configs: LooseObject) {
        console.log('>> Starting Bullmq config ...',);
        this._connection = {
            host: configs.REDIS_HOST,
            port: parseInt(configs.REDIS_PORT, 10,),
            password: configs.REDIS_PASSWORD,
            db: configs.REDIS_DB,
        };
        this._flowbuildQueues = {};
        this._flowbuildWorkers = {};
        console.log('>> Starting Bullmq configured.',);
    }

    async connect(consumesFrom: Array<string>, producesTo: Array<string>, callback:any,) {
        if (consumesFrom.length) {
            console.log('[CONNECT] Bullmq creating consumers ...',);
            await this.setConsumer(consumesFrom, callback,);
            console.log('[CONNECT] Bullmq consumers created.',);
        }
        if (producesTo.length) {
            console.log('[CONNECT] Bullmq creating producers ...',);
            await this.setProduces(producesTo,);
            console.log('[CONNECT] Bullmq producers created.',);
        }
    }

    async setConsumer(topics: Array<string>, callback: any,){
        for await (const topic of topics){
            this._flowbuildWorkers[topic] = new Worker(
                topic, 
                this.mountConsumerCallback(callback,), 
                {connection: this._connection,},
            );
            console.log(`[CONSUMER] Bullmq consumer for "${topic}" created.`,);
        }
    }

    async setProduces(topics: Array<string>,){
        for (const topic of topics) {
            this._flowbuildQueues[topic] = new Queue(
                topic, 
                {connection: this._connection,},
            );
            console.log(`[PRODUCER] Bullmq producer for "${topic}" created.`,);
        }
    }

    async produce({ topic, message, }: { topic: string; message: LooseObject },){
        console.log('[PRODUCE] Publishing message ... ', { topic, message, },);
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
                console.log('[PRODUCE] Message published.',);
                return true;
            } catch (error) {
                console.error('[PRODUCE] Error publishing message.', error,);
                return false;
            }
        }
        return false;
    }
  
    mountConsumerCallback(callback: any,){
        return async (job: Job,) => {
            const receivedMessage = job.data;
            console.log(`[CONSUMER] Message received (bullmq) -> ${JSON.stringify(
                {
                    value: receivedMessage,
                },
            )}`,
            );
            callback(job.name, receivedMessage,);
        };
    }
}
