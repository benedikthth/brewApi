var express = require("express");
var bodyParser = require("body-parser");
var app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }))

var Clickhouse = require('@apla/clickhouse');


var mqttHandler = require('./MqttHandler');


var ch = new Clickhouse('localhost', {
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
    res.header('Access-Control-Allow-Origin' , "brew.spock.is");

    //const limit = req.query.limit || 96;
    //should be in hours....
    const hourLimit = req.query.hourLimit || null; // one week.
    let whereClause = ""; 
    
    if(hourLimit !== null){
        const secondsofDelay = hourLimit * 60 * 60;
        whereClause = `WHERE dtime >= ( minus(now(), ${secondsofDelay}))`
    }

    //max 144 rows. this means that we'll only get 3days worth of data
    //var stream = ch.query (`SELECT toStartOfMinute(dtime) as dtime, temperature FROM ${tempTable} order by dtime desc limit ${limit}`);
    var stream = ch.query (`SELECT toStartOfMinute(dtime) AS dtime, temperature 
                            FROM ${tempTable} 
                            ${whereClause}
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

app.get('/temperature/average', function(req, res){

    const hourLimit = req.query.hourLimit || null; // one week.
    let whereClause = ""; 
    
    if(hourLimit !== null){
        const secondsofDelay = hourLimit * 60 * 60;
        whereClause = `WHERE dtime >= ( minus(now(), ${secondsofDelay}))`
    }
    let query = `
    SELECT avg(temperature) as avg
    FROM brewApi_temperatures
    ${whereClause}
    `

    let stream = ch.query(query);

    //res.write('[');

    let metadata;
    stream.on('metadata', data => (metadata = data.map(d => d.name)));

    //let first = true;
    stream.on('data', data => {

        const o = metadata.reduce((p, k, i)=> {
            p[k] = data[i];
            return p;
        }, {});
        
        //res.write((first ? '' : ', ') + JSON.stringify(o));
        res.write(JSON.stringify(o));
        first = false;
    });

    stream.on ('error', function (err) {
        // TODO: handler error
        console.log('ERROR: ');
        console.log(err);
        
        //res.send('error');
    });
    
    stream.on('end', ()=> res.end());


});

app.get('/temperature/regression', (req, res)=>{

    let whereClause = `where dtime > (now() - (60*60*2))`;
    const minuteLimit = req.query.minuteLimit || null; // one week.
    
    if(minuteLimit !== null){
        const secondsofDelay = minuteLimit * 60;
        whereClause = `WHERE dtime >= ( minus(now(), ${secondsofDelay}))`
    }

    let query = `
    select corr(toRelativeSecondNum(dtime), temperature) as corr,
           stddevSamp(temperature)  as stddevY,
           stddevSamp(toRelativeSecondNum(dtime))  as stddevX,
           avg(toRelativeSecondNum(dtime)) as meanX,
           avg(temperature) as meanY,
           min(toRelativeSecondNum(dtime)) as minX
    FROM (select * from brewApi_temperatures 
    ${whereClause})`
    //stddevSamp(temperature)  as stddevY from (select temperature from brewApi_temperatures where dtime > (now() - (60*60) ) ) ,
    let stream = ch.query(query);

    let metadata;
    stream.on('metadata', data => (metadata = data.map(d => d.name)));

    //let first = true;
    stream.on('data', data => {

        const o = metadata.reduce((p, k, i)=> {
            p[k] = data[i];
            return p;
        }, {});
        
        //res.write((first ? '' : ', ') + JSON.stringify(o));
        res.write(JSON.stringify(o));
        first = false;
    });

    stream.on ('error', function (err) {
        // TODO: handler error
        console.log('ERROR: ');
        console.log(err);
        
        //res.send('error');
    });
    
    stream.on('end', ()=> res.end());
    

})

var server = app.listen(9099, function () {
    console.log("app running on port.", server.address().port);
});




