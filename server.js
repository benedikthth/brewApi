var express = require("express");
var bodyParser = require("body-parser");
var app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }))

var Clickhouse = require('@apla/clickhouse');


var mqttHandler = require('./MqttHandler');


var ch = new Clickhouse('192.168.1.101', {
    dataObjects: true
});//'192.168.1.101');

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

        //todo:! 
        //console.log(err);
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


    //const limit = req.query.limit || 96;
    //should be in hours....
    const dateLimit = req.query.dateLimit || 24 * 7; // one week.
    const secondsofDelay = dateLimit * 60 * 60;
    //max 144 rows. this means that we'll only get 3days worth of data
    //var stream = ch.query (`SELECT toStartOfMinute(dtime) as dtime, temperature FROM ${tempTable} order by dtime desc limit ${limit}`);
    var stream = ch.query (`SELECT toStartOfMinute(dtime) AS dtime, temperature 
                            FROM ${tempTable} 
                            WHERE dtime >= ( minus(now(), ${secondsofDelay}))
                            ORDER BY dtime desc `
                            );
    

    res.write('[');

    let metadata;
    stream.on('metadata', data => (metadata = data.map(d => d.name)));

    let first = true;
    stream.on('data', data => {

        const o = metadata.reduce((p, k, i)=> {
            p[k] = data[i];
            return p;
        }, {});
        
        res.write((first ? '' : ', ') + JSON.stringify(o));
        first = false;
    });

    stream.on ('error', function (err) {
        // TODO: handler error
        console.log('ERROR: ');
        console.log(err);
        
        //res.send('error');
    });
    
    stream.on('end', ()=> res.end(']'));
    
});

var server = app.listen(9099, function () {
    console.log("app running on port.", server.address().port);
});




