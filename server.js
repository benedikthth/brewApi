var express = require("express");
var bodyParser = require("body-parser");
var app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }))

var Clickhouse = require('@apla/clickhouse');


var mqttHandler = require('./MqttHandler');


var ch = new Clickhouse('192.168.1.101');//'192.168.1.101');

//name of table.
const tempTable = 'brewApi_temperatures';
const co2Table = 'brewApi_co';

// Connection URL


// Use connect method to connect to the server
var messageEvent = function(topic, msg) {


    let table = '';
    let varname = ''
    if (topic === '/brewApi/temperature'){
        table = tempTable;
        varname = 'temperature'
    } else if ( topic === '/brewApi/co2'){
        table = co2Table;
        varname = 'level'
    }
    let query = `INSERT INTO ${table} (${varname}) VALUES ( ${msg} )`;

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
    var stream = ch.query (`SELECT (dtime, temperature) FROM ${tempTable} order by dtime desc limit 144`);
    
    let metadata;

    stream.on('metadata', data => (metadata = data));

    res.write('[');

    stream.on('data', data => res.write(JSON.stringify(data) + ', '));

    stream.on ('error', function (err) {
        // TODO: handler error
        console.log('ERROR: ');
        console.log(err);
        
        //res.send('error');
    });
    
    stream.on('end', ()=> res.end('null ]'));
    
    //let data = [];
    /*
    stream.on ('data', function (row) {
        //[time, dtime, dateString, temperature];
        let o = {
            dtime: row[1],
            temperature: row[2]
        }
        data.push (o);
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
        res.send(JSON.stringify(data));
        
      });
      */

    
});

var server = app.listen(9099, function () {
    console.log("app running on port.", server.address().port);
});




