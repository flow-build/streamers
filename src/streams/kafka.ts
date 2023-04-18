import { Consumer, Kafka, Producer, SASLOptions, KafkaConfig} from 'kafkajs';
import { LooseObject, ProduceParam, } from '../types';
import { v4 as uuid, } from 'uuid';
import { EachMessagePayload, } from 'kafkajs';


export class KafkaStream {
    _client: any;
    _producer: Producer;
    _consumer: Consumer;
    _callback: any;
  
    constructor(configs: LooseObject) {
        console.log('[Kafka CONFIG] Starting configuration ...');
        this._client = KafkaStream.createClient(configs);
        this._producer = this._client.producer();
        this._consumer = this._client.consumer({
            groupId: `${configs.GROUP_CONSUMER_ID}-consumer-group`,
        });
        console.log('[Kafka CONFIG] configurated.');
    }

    static createClient(configs: LooseObject){
        const connectionConfig: KafkaConfig = {
            clientId: `${configs.CLIENT_ID}-${uuid()}`,
            brokers: [`${configs.BROKER_HOST}:${configs.BROKER_PORT}`],
        };
        if (configs.BROKER_KAFKA_MECHANISM && configs.BROKER_KAFKA_USERNAME && configs.BROKER_KAFKA_SECRET) {
            const sasl: SASLOptions = {
                mechanism: configs.mechanism,
                username: configs.username,
                password: configs.pswd,
            }; 
            connectionConfig.sasl = sasl;
            connectionConfig.ssl = !!sasl;
        }
        return new Kafka(connectionConfig);
    }

    async connect(consumesFrom: Array<string>, producesTo: Array<string>, callback: any){
        if (consumesFrom.length) {
            console.log('[Kafka CONNECT] Consumer connecting ...');
            await this._consumer.connect();
            console.log('[Kafka CONNECT] Consumer connected ... setting consumers ...');
            await this.setConsumer(consumesFrom);
            this._callback = this.mountConsumerCallback(callback);
            await this.runConsumer();
        }
        if (producesTo.length) {
            console.log('[Kafka CONNECT] Producer connecting ...');
            await this._producer.connect();
            console.log('[Kafka CONNECT] Producer connected');
        }
    }

    async setConsumer(consumesFrom: Array<string>){
        for (const topic of consumesFrom){
            console.log(`[Kafka CONSUMER] Subscribing consumer for "${topic}" ...`);
            // Consume Dynamic Topics
            let cleanTopic: string | RegExp = topic;
            if(topic.indexOf("$")!=-1) {
                cleanTopic = new RegExp(topic.replace("$", ".*"));
            }
            await this._consumer.subscribe({
                topic: cleanTopic,
                fromBeginning: true,
            });
            console.log(`[Kafka CONSUMER] Consumer subscribed for "${topic}".`);
        }
    }

    async runConsumer(){
        await this._consumer.run({
            eachMessage: this._callback,
        });
        console.log(`[Kafka CONSUMER] Consumer running. `);
    }

    async produce({ topic, message, options }: ProduceParam){
        if(options?.delay){
            await setTimeout(options?.delay);
        }
        await this._producer.send({
            topic,
            messages: [{ value: JSON.stringify(message) }],
        });
    }

    mountConsumerCallback(callback: any){
        return async ({ topic, partition, message, }: EachMessagePayload): Promise<void> => {
            const receivedMessage = message.value?.toString() || '';
            console.log(`[Kafka CONSUMER] Message received -> ${JSON.stringify(
                {
                    partition,
                    offset: message.offset,
                    value: receivedMessage,
                },
            )}`);
            callback(topic, receivedMessage);
        };
    }
}
