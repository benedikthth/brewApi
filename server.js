var express = require("express");
var bodyParser = require("body-parser");
var app = express();
var cors = require('cors')

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }))
app.use(cors())
var mqttHandler = require('./MqttHandler');

// Connection URL
/** PSQL CONNECTION SETUP  */
const {Client } = require("pg")
const connectionString = 'postgresql://node:password@192.168.1.101:5432/nodedb'
const client = new Client({
    connectionString: connectionString
})
client.connect()



// Use connect method to connect to the server
var messageEvent = function(topic, msg) {

    client.query(`INSERT INTO temperatures (temperature) VALUES (${Number(msg)})`, (err, res)=>{

    });

}


var mqttClient = new mqttHandler(messageEvent);
mqttClient.connect();

mqttClient.subscribe('brewApi/temperature');
//mqttClient.subscribe('/brewApi/humidity');
// Routes
app.post("/send-mqtt", function(req, res) {
    mqttClient.sendMessage(req.body.message);
    res.status(200).send("Message sent to mqtt");
});

app.get('/temperature', function(req, res){
    

    // console.log(typeof(req.query.hourLimit))
    if(typeof(req.query.hourLimit)!== 'undefined'){
        client.query(`SELECT * from temperatures where time > (NOW() - INTERVAL '${req.query.hourLimit} hours') ORDER BY time DESC`, (x, y)=>{
            res.status(200).send(JSON.stringify(y.rows))
        })
    } else {
        client.query(`SELECT * from temperatures  ORDER BY time DESC`, (x, y)=>{
            res.status(200).send(JSON.stringify(y.rows))
        })
    }


});



/** Returns the lin-regression  */
app.get('/temperature/regression', (req, res)=>{


    client.query(
    `SELECT 
        regr_slope(temperature, extract(epoch from time )) as slope
    FROM temperatures where time > (NOW() - INTERVAL '30 minutes')`, 
    (err, y)=>{
        if(err){
            // console.log(err);
        } else {
            res.send(y.rows[0]);
        }
    })

    
})

var server = app.listen(9099, function () {
    console.log("app running on port.", server.address().port);
});




