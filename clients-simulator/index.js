let fs = require("fs");
let kafka = require("kafka-node");

/* 
* required:
* _id, timestamp, device_platform, http_referrer,  item, quantity,  value
* want to have this output 
*/
let config = JSON.parse(fs.readFileSync("config.json"));
let refereesJson = JSON.parse(fs.readFileSync("resources/referrers.json"));
let platformsJson = JSON.parse(fs.readFileSync("resources/web-clients.list.json"));
let platforms = platformsJson["userAgents"].map(element =>{
    return element.deviceName + "-" +element.value;
 })

 let client = new kafka.Client(config.output.kafka.host+":"+config.output.kafka.port+"/")
let producer = new kafka.Producer(client);


 var _id = 1;
 var Csv = [];
 var quantities = [0];
for (var i = 0; i < config.purchases.iterations;){
    quantities.push (++i)
}


 _init();

function _init(){    
    let stockJson = JSON.parse(fs.readFileSync(config.stock.path));    
    let max_i = config.purchases.max;
    var i = 0; // max_i*stocks.electronics.phones = num rows in Csv
    while (i++ < max_i){
        for (var k in stockJson){
            stockJson[k].forEach(addToJsonRandomRecord);
        }
    }
    //writeToJson(Csv);
    writeToKafka()
}

function writeToJson(){
    if(!arguments[0])
        return;
    var data = arguments[0];
    /*for (var i in arguments[0]){
        data += arguments[0] + "\n";
    }*/
    fs.writeFileSync(config.output.fs.path + "/" +config.output.fs.file,data);

}
function writeToKafka(){
    handleRecord(0);
    producer.on('ready', function () {
        console.log("Producer is ready");
    });
    producer.on('error', function (err) {
        console.error("Problem with producing Kafka message "+err);
    })
}
function handleRecord(currentRecord) {   
    let line = Csv[currentRecord];
    var record = ""
    for (var i in line) {
        record += (i>0)?config.Csv.delimiter:"";
        record += line[i] ;
    }
     console.log(record);

     produceRecordMessage(record)
     let delay = config.output.kafka.timer.delay.avg + (Math.random() -0.5) * config.output.kafka.timer.delay.spread;
     setTimeout(handleRecord.bind(null, cycleIndex(currentRecord,Csv)), delay);
}
function produceRecordMessage(record) {
    payloads = [
        { topic: config.output.kafka.topic, messages: record, partition: 0 },
    ];
    producer.send(payloads, (err, data) => {
        if(typeof(data) !== "undefined")
            console.log(data)
        else {
            console.log(err["message"])
        }
    });
}
function addToJsonRandomRecord(stockElement){
    let refereesIndex = Math.floor(Math.random()*10) % refereesJson.links.length; 
    let platformsIndex = Math.floor(Math.random()*10) % platforms.length; 
    let quantityIndex = Math.floor(Math.random()*10) % 2;

    let timestamp = getRandomTime(true);
    _id += 1;
    
    /*Csv.push(
        {
            "_id": _id,
            "timestamp" : timestamp,
            "platform" : platforms[platformsIndex].replace(",","-"), "referer":refereesJson.links[refereesIndex],
            "item":stockElement.name,
            "quantity": quantities[quantityIndex],
            "price": stockElement.price
    });*/
    Csv.push(Array(
        _id,
        timestamp,
        platforms[platformsIndex].replace(",","-"),
        refereesJson.links[refereesIndex],
        stockElement.name,
        quantities[quantityIndex],
        stockElement.price
    ))

}
function getRandomTime(){
    let years = [2016,2017,2018];
    let yrIndex = Math.floor(Math.random()*10)%2;
    let year = years[yrIndex];
    
    let today = new Date();
    var monthDivisor = 12;
    if(today.getFullYear() == year)
        monthDivisor = today.getMonth();

    let month = Math.floor(Math.random()*100)%monthDivisor;
    let day = Math.floor(Math.random()*100)%31;
    let hour = Math.floor(Math.random()*100)%24;
    let min = Math.floor(Math.random()*100)%60;
    let sec = Math.floor(Math.random()*100)%60;

    let date = new Date(year,month,day,hour,min,sec);
    if(arguments[0])
        return Math.floor(date.getTime()/1000); //return unix timestamp in seconds
    return date.getTime(); //return unix timestamp in milliseconds
}

function cycleIndex(index, array){
    if(index < array.length - 1)
        return index + 1;
    return 0;
}

