let fs = require("fs");
let kafka = require("kafka-node");

/* 
* reads data from the kafka output and charts it 
*/
let config = JSON.parse(fs.readFileSync("config.json"));

let client = new kafka.Client(config.kafka.host+":"+config.kafka.port+"/")

const consumer = new kafka.HighLevelConsumer(client,config.kafka.topics, config.options)

consumer.on("message",function(message){
    var buffer = new Buffer(message.value,"binary");
    var decMessage = JSON.parse(buffer.toString());

    return EventSource.create({
        id : decMessage.id,
        type : decMessage.type,
        userId : decMessage.userId,
        sessionId: decMessage.sessionId,
        data : JSON.stringify(decMessage.data),
        createdAt : new Date()
    })
})

consumer.on("error",function(err){
    console.log("error", err);
})
process.on("SIGINT",function(){
    consumer.close(true,function(){
        process.exit();
    })
})


