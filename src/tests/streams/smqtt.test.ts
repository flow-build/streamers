/* eslint-disable no-unused-vars */
/* eslint-disable no-undef */
import { MqttStream } from '../../streams/smqtt';

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

    it('Check if base case is working', async () => {
        const consumerCallback = (topic: string, receivedMessage: string,) => {};
        stream = new MqttStream({
            'MQTT_HOST': 'localhost',
            'MQTT_PORT': '1883',
            'MQTT_PROTOCOL': 'http',
            'MQTT_USERNAME': 'admin',
            'MQTT_PASSWORD': 'hivemq',
        });
        //MqttStream._connectClient = jest.fn().mockReturnValue(new MqttMock());
        await stream.connect();
        for (const topic of ["process-topic", "process-dynamic-$"]){
            await stream.setConsumer(topic,consumerCallback);
            await stream.runConsumer();
            expect(stream._client.subscribe).toHaveBeenCalled();
            expect(stream._client.on).toHaveBeenCalled();
        }

        await stream.produce({
            "topic":"process-topic", 
            "message":{"mensagem": "This is an test"},
        });
        expect(stream._client.publish).toHaveBeenCalled();
        await stream.produce({
            "topic":"process-dynamic-topic", 
            "message":{"mensagem": "This is an test for dynamic"},
        });
        expect(stream._client.publish).toHaveBeenCalled();
    
    },);
},);
