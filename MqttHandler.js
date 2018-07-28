const mqtt = require('mqtt');

class MqttHandler {
  constructor(callback) {
    
    this.mqttClient = null;
    this.host = 'mqtt://192.168.1.101';
    this.callback = callback;
  }
  
  subscribe(topic){
    this.mqttClient.subscribe(topic, {qos: 0});
  }


  connect() {
    
    // Connect mqtt with credentials (in case of needed, otherwise we can omit 2nd param)
    this.mqttClient = mqtt.connect(this.host);

    // Mqtt error calback
    this.mqttClient.on('error', (err) => {
      console.log(err);
      this.mqttClient.end();
    });

    // Connection callback
    this.mqttClient.on('connect', () => {
      console.log(`mqtt client connected`);
    });

    // mqtt subscriptions

    // When a message arrives, console.log it
    this.mqttClient.on('message', function (topic, message) {
      this.callback(topic, message);
    }.bind(this));

    this.mqttClient.on('close', () => {
      console.log(`mqtt client disconnected`);
    });
  }
  
  // Sends a mqtt message to topic: mytopic
  sendMessage(message) {
    this.mqttClient.publish('test', message);
  }
}

module.exports = MqttHandler;
