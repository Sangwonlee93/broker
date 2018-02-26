var mosca = require('mosca');
var mqtt = require('mqtt');
var request = require('request');
var dateTime = require('node-datetime');
var bridge = "";
var clusterServer = process.env.cluster;
var dbServer = process.env.dbhost;
var connectManager;
var brokerId = process.env.brokerid;
var settings = {
  port: 1883
};
var server = new mosca.Server(settings);
server.on('ready', setup); //on init it fires up setup()

// fired when the mqtt server is ready
function setup() {
  console.log('Borker is up and running');
  connectManager = mqtt.connect(clusterServer);
  bridge = connectManager.options.clientId;
  connectManager.on('connect', () => {
    connectManager.subscribe('#');
  });
  connectManager.on('message', (topic, message) => {
    var slashindex = topic.indexOf("/") + 1;
    if (brokerId != topic.slice(0, slashindex - 1)) {
      var temp = topic.slice(slashindex, topic.length);
      var tempslash = temp.indexOf("/")+1;
      var realmsg = {
        topic: temp.slice(tempslash, temp.length),
        payload: message, // or a Buffer
        qos: 1, // 0, 1, or 2
        retain: false, // or true
      };
      server.publish(realmsg, "ComeToBrokerManager", function() {
      });
    }
  });
}
server.on('published', function(packet, client) {
  if (packet.topic.indexOf("$SYS") != 0 && packet.topic.indexOf("broadcast")==0) {
    connectManager.publish(brokerId + "/" + packet.topic, packet.payload);
  }
});
// fired when a client connects
server.on('clientConnected', function(client) {
  if (bridge != client.id) {
    var dt = dateTime.create();
    var formatted = dt.format('Y-m-d H:M:S');
    var options = {
      uri: 'http://'+dbServer+':8080/client',
      method: 'POST',
      json: {
        "client_mqtt_id": client.id,
        "last_connected": formatted,
        "broker_id": brokerId
      }
    };

    request(options, function(error, response, body) {
      if (!error && response.statusCode == 200) {
      }
    });
  }
});

// fired when a client disconnects
server.on('clientDisconnected', function(client) {
  if (bridge != client.id) {
    var options = {
      uri: 'http://'+dbServer+':8080/client',
      method: 'DELETE',
      json: {
        "client_mqtt_id": client.id
      }
    };

    request(options, function(error, response, body) {
      if (!error && response.statusCode == 200) {
      }
    });
  }
});
