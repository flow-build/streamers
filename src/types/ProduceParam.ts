import { LooseObject } from "./LooseObject.type";

export type ProduceParam = { 
    topic: string; 
    message: LooseObject; 
    options?: LooseObject;
}