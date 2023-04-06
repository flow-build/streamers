import { LooseObject, } from './types';

export class Streamer {
    name: string;
    streamClass: any;
    stream?: any;
    topics: LooseObject = {
        consumesFrom: [],
        producesTo: [],
    };
    configs?: LooseObject;

    constructor(name: string, streamClass:any,) {
        this.name = name;
        this.streamClass = streamClass;
    }
}