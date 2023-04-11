/* eslint-disable no-unused-vars */
/* eslint-disable no-undef */
import { MqttStream } from '../../streams/mqtt';

class MqttMock {
    subscribe = jest.fn((topic) => {});
    on = jest.fn(({ topic, callback }) => {});
    publish = jest.fn(({ topic, message }) => {});
}

jest.mock("mqtt", () => {
    return {
        connect(configs: any) { return new MqttMock(); }
    };
});

describe('Mqtt Stream suite test', () => {
    let stream: MqttStream;

    beforeEach(() => {
        jest.clearAllMocks();
    });

    it('Check if base case is working', async () => {
        const consumerCallback = (topic: string, receivedMessage: string,) => {};
        stream = new MqttStream({
            'MQTT_HOST': 'localhost',
            'MQTT_PORT': '1883',
            'MQTT_PROTOCOL': 'http',
            'MQTT_USERNAME': 'admin',
            'MQTT_PASSWORD': 'hivemq',
        });
        await stream.connect(
            ["process-topic"], 
            ["process-topic"], 
            consumerCallback
        );
        expect(stream._client.subscribe).toHaveBeenCalledTimes(1);
        expect(stream._client.on).toHaveBeenCalledTimes(1);

        let result = await stream.produce({
            "topic":"process-topic", 
            "message":{"mensagem": "This is an test"},
        });
        expect(stream._client.publish).toHaveBeenCalledTimes(1);
        expect(result).toEqual(true);

        stream._client.publish = jest.fn(() => { throw new Error("My Error"); });
        result = await stream.produce({
            "topic":"process-topic", 
            "message":{"mensagem": "This is an test"},
        });
        expect(result).toEqual(false);
    
    },);
},);
