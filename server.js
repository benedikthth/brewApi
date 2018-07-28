var express = require("express");
var bodyParser = require("body-parser");
var app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }))

var Clickhouse = require('@apla/clickhouse');


var mqttHandler = require('./MqttHandler');


var ch = new Clickhouse('localhost');//'192.168.1.101');

//name of table.
const brewApiTemperatures = 'brewApi_temperatures';


// Connection URL


// Use connect method to connect to the server
var messageEvent = function(topic, msg) {


    let datet = "" + new Date().toISOString().replace('T', ' ').replace('Z', '');
    let date = datet.split(' ')[0];
    console.log(date, typeof date);
    let table = '';
    if (topic === '/brewApi/temperature'){
        table = brewApiTemperatures;
    }
    let query = `INSERT INTO ${table} (temperature) VALUES ( ${msg} )`;

    ch.query ( query , function (err, data) {
        console.log(err);
    });

}


var mqttClient = new mqttHandler(messageEvent);
mqttClient.connect();

mqttClient.subscribe('/brewApi/temperature');
//mqttClient.subscribe('/brewApi/humidity');
// Routes
app.post("/send-mqtt", function(req, res) {
    mqttClient.sendMessage(req.body.message);
    res.status(200).send("Message sent to mqtt");
});

app.get('/temperature', function(req, res){
    //select * from brewApi_temperatures order by dtime desc limit 60

    //max 144 rows. this means that we'll only get 3days worth of data
    var stream = ch.query (`SELECT * FROM ${brewApiTemperatures} order by dtime desc limit 144`);
    let rows = [];
    
    stream.on ('data', function (row) {
        rows.push (row);
      });
      
      stream.on ('error', function (err) {
        // TODO: handler error
        console.log('ERROR: ');
        console.log(err);
        
        res.send('error');
      });
      
      stream.on ('end', function () {
        // all rows are collected, let's verify count
        //(rows.length === stream.supplemental.rows);
        // how many rows in result are set without windowing:
        //console.log ('rows in result set', stream.supplemental.rows_before_limit_at_least);
        res.send(rows);
        
      });

});

var server = app.listen(9099, function () {
    console.log("app running on port.", server.address().port);
});




