import { LooseObject, ProduceParam, } from './types';


export class Streamer {
    name: string;
    label: string;
    brokerClass: any;
    broker?: any;
    topics: LooseObject = {
        consumesFrom: [],
        producesTo: [],
        producesToDynamic: []
    };
    configs?: LooseObject;
    callback?: Function;

    constructor(name: string, brokerClass:any) {
        this.name = name;
        this.brokerClass = brokerClass;
        this.label = this.name.toUpperCase();
    }

    mountTopics(topics: LooseObject){
        for (const [topic, tconfig] of Object.entries(topics) as unknown as any) {
            if(tconfig.consumesFrom && tconfig.consumesFrom.includes(this.name)) {
                this.topics.consumesFrom.push(topic);
            }
            if(tconfig.producesTo && tconfig.producesTo.includes(this.name)) {
                if (topic.indexOf("$")!=-1){
                    this.topics.producesToDynamic.push(new RegExp(topic.replace("$", ".*")));
                } else {
                    this.topics.producesTo.push(topic);
                }
            }
        }
    }

    hasTopics(){
        return this.topics?.consumesFrom.length || 
        this.topics?.producesTo.length ||
        this.topics?.producesToDynamic.length;
    }
    
    async startStream(configs: LooseObject, callback?: any){
        if (this.hasTopics()) {
            this.callback = callback;
            this.configs = configs;
            this.broker = new this.brokerClass(this.configs);
            await this.connect();
        } else {
            console.error(`[${this.label} CONNECT] Stream has no topics.`);
        }
    }

    async connect(){
        try {
            if(!this.broker) return false;
            return await this.broker.connect(
                this.topics?.consumesFrom,
                this.topics?.producesTo,
                this.callback,
            );
        } catch (error) {
            console.error(`[${this.label} CONNECT] Error connect to broker.`, error);
        }
    }

    async validateProduceConfig(topic: string){
        if(!this.configs) return false;
        if (this.topics.producesTo.includes(topic)) return true;
        for await (const produceTopic of this.topics.producesToDynamic) {
            return topic.match(produceTopic);
        }
        console.log(`[${this.label} PRODUCE] No config for ${topic}.`);
        return false;
    }

    async produce(params: ProduceParam){
        if (!await this.validateProduceConfig(params.topic)) return false;
        console.log(`[${this.label} PRODUCE] Publishing message ... `);
        if (!params.options) {
            params.options = {};
        }
        try {
            if(!this.broker) return false;
            await this.broker.produce(params);
            console.log(`[${this.label} PRODUCE] Message published.`);
            return true;
        } catch (error) {
            console.error(`[${this.label} PRODUCE] Error publishing message.`, error);
            return false;
        }
    }
}