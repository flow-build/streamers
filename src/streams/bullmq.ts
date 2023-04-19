import { Job, JobsOptions, Queue, Worker } from 'bullmq';
import { LooseObject, ProduceParam } from '../types';


export class BullmqStream {
    _connection: LooseObject;
    _flowbuildQueues: { [key: string]: Queue };
    _flowbuildWorkers: { [key: string]: Worker };
    _queueConfig: JobsOptions = {
        removeOnComplete: true, 
        removeOnFail: false,
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 1000,
        },
    };
  
    constructor(configs: LooseObject) {
        this._connection = {
            host: configs.REDIS_HOST,
            port: parseInt(configs.REDIS_PORT, 10),
            password: configs.REDIS_PASSWORD,
            db: configs.REDIS_DB,
        };
        this._flowbuildQueues = {};
        this._flowbuildWorkers = {};
    }
    
    async connect(){
    }

    // eslint-disable-next-line no-unused-vars
    async setConsumer(topic: string, callback: any, dynamicKey = "$"){
        this._flowbuildWorkers[topic] = await this.createWorker(topic, callback);
    }

    async runConsumer(){
    }

    async createWorker(topic: string, callback: any){
        return new Worker(
            topic, this.mountConsumerCallback(callback), 
            {connection: this._connection,},
        );
    }
    
    async createQueue(topic: string){
        return new Queue(topic, {connection: this._connection});
    }

    async getOrCreateFlowbuildQueue(topic: string){
        let queue = undefined;
        if (topic in this._flowbuildQueues){
            queue = this._flowbuildQueues[topic];
        } else {
            queue = await this.createQueue(topic);
            this._flowbuildQueues[topic] = queue;
        }
        return queue;
    }

    async produce({ topic, message }: ProduceParam){
        let queue = await this.getOrCreateFlowbuildQueue(topic);
        await queue.add(
            topic, 
            JSON.stringify(message), 
            this._queueConfig,
        );
    }
    
    mountConsumerCallback(callback: any){
        return async (job: Job) => {
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
