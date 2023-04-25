import { LooseObject, ProduceParam, } from './types';


class Topics{
    consumesFrom: Array<string>;
    producesTo: Array<string>;
    producesToDynamic: Array<RegExp>;
    dynamicKey = "$";

    constructor() {
        this.consumesFrom = [];
        this.producesTo = [];
        this.producesToDynamic = [];
    }

    hasTopics(){
        return this.consumesFrom.length || 
        this.producesTo.length ||
        this.producesToDynamic.length;
    }

    _isDynamicTopic(topic: string){
        return topic.indexOf(this.dynamicKey)!=-1;
    }

    _convertDynamicTopic(topic: string){
        return new RegExp(topic.replace(this.dynamicKey, ".*"));
    }

    mountTopics(streamName: string, topics: LooseObject){
        for (const [topic, tconfig] of Object.entries(topics) as unknown as any) {
            if(tconfig.consumesFrom && tconfig.consumesFrom.includes(streamName)) {
                this.consumesFrom.push(topic);
            }
            if(tconfig.producesTo && tconfig.producesTo.includes(streamName)) {
                if (this._isDynamicTopic(topic)){
                    this.producesToDynamic.push(this._convertDynamicTopic(topic));
                } else {
                    this.producesTo.push(topic);
                }
            }
        }
    }
}

export class Streamer {
    name: string;
    label: string;
    brokerClass: any;
    broker?: any;
    topics = new Topics();
    configs?: LooseObject;
    callback?: Function;

    constructor(name: string, brokerClass:any) {
        this.name = name;
        this.brokerClass = brokerClass;
        this.label = this.name.toUpperCase();
    }
    
    async startStream(topics: LooseObject, configs: LooseObject, callback?: any){
        this.callback = callback;
        this.configs = configs;
        this.topics.mountTopics(this.name, topics);
        if (this.topics.hasTopics()) {
            await this.configBroker();
            await this.prepareConsumers();
        } else {
            console.error(`[${this.label} CONNECT] Stream has no topics.`);
        }
    }

    async configBroker(){
        console.log(`[${this.label} CONFIG] Starting Broker configuration ...`);
        this.broker = new this.brokerClass(this.configs);
        console.log(`[${this.label} CONNECT] Starting connection ...`);
        await this.broker.connect();
        console.log(`[${this.label} CONFIG] Broker connected and onfigurated.`);
    }

    async prepareConsumers(){
        console.log(`[${this.label} CONSUMER] Preparing Consumers ....`);
        try {
            if (this.topics?.consumesFrom.length) {
                for (const topic of this.topics.consumesFrom){
                    console.log(`[${this.label} CONSUMER] Preparing consumers for "${topic}".`);
                    await this.broker.setConsumer(topic, this.callback);
                }
            }
            await this.broker.runConsumer();
            console.log(`[${this.label} CONSUMER] Consumers Prepared.`);
        } catch (error) {
            console.error(`[${this.label} CONSUMER] Error preparing consumers.`, error);
        }
    }

    async validateProduceConfig(topic: string){
        if (!this.configs) {
            console.log(`[${this.label} PRODUCE] Stream not configured.`);
            return false;
        }
        if (!this.broker) {
            console.log(`[${this.label} PRODUCE] Broker not found.`);
            return false;
        }
        if (this.topics.producesTo.includes(topic)) return true;
        for await (const produceTopic of this.topics.producesToDynamic) {
            return topic.match(produceTopic);
        }
        return false;
    }

    async produce(params: ProduceParam){
        console.log(`[${this.label} PRODUCE] Publishing message ... `);
        if (!await this.validateProduceConfig(params.topic)) return false;
        if (!params.options) {
            params.options = {};
        }
        try {
            await this.broker.produce(params);
            console.log(`[${this.label} PRODUCE] Message published.`);
            return true;
        } catch (error) {
            console.error(`[${this.label} PRODUCE] Error publishing message.`, error);
            return false;
        }
    }
}