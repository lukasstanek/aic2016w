var kafka = require('kafka-node');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var WebSocket = require('ws');
var loki = require('lokijs');


class KafkaSocketServer{
	constructor(){
		this.WebSocketServer = {};
		this.wss = {};
		this.ws = [];
		
		this.topic = 'BoltOutput';

		this.client = {};
		this.consumer = {};
		
		this.db = new loki('taxi.db');
		this.taxis = this.db.addCollection('taxis');
		this.totalDistance = 0;
		this.totalTaxis = 0;
	}
	
	
	listen(){
		this.connectToKafka();
		
		this.WebSocketServer = WebSocket.Server;
		this.wss = new this.WebSocketServer({port: 8080, perMessageDeflate: false});
		console.log('listening on web socket');

		this.wss.on('connection', (ws) => {
			var index = this.ws.push(ws) - 1;
			ws.on('close', () => {
				console.log(`client ${index} disconnected`)
				this.ws.splice(index, 1)
			})
		})
		
	}


	connectToKafka(){
		this.client = new Client('localhost:2181')
		var topics = [{topic: this.topic, partition: 0}]
		var options = {autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024*1024};

		this.consumer = new Consumer (this.client, topics, options);
	        this.offset = new Offset(this.client);

		this.consumer.on('message', (message) => {
			this.processOutput(message);
		});
	}

	
	processOutput(message){
		var fields = message.value.split(",");
		switch(fields[1]){
			case "NotifyOutOfBoundsBolt":
				if(fields[2] === ">10"){
					this.updateTaxiAreaViolation(fields[0], true);
				}
				if(fields[2] === "<10"){
					this.updateTaxiAreaViolation(fields[0], false);
				}
				if(fields[2] === ">15"){
					this.updateTaxiOutOfBounds(fields[0], true);
				}
				if(fields[2] === "<15"){
					this.updateTaxiOutOfBounds(fields[0], false);
				}
			break;
			case 'NotifySpeedingBolt':
				this.broadcast(JSON.stringify({ id: fields[0], type:'message' ,msg: fields[2]}));
			break;
			case 'TaxiTotal':
				this.updateCurrentNumTaxisDriving(fields[2]);
			break;
			case 'DistanceTotal':
				this.updateOverallDistance(fields[2]);
			break;
			case 'LocationBolt':
				this.updateTaxiLocation(fields[0], parseFloat(fields[2]), parseFloat(fields[3]));
			break;
				
		}
	}


	broadcast(message){
		this.ws.forEach((client) => {
			client.send(message.toString());
		});
	}
		
	updateTaxiLocation(id, longi, lati){
		var taxi = this.taxis.findOne({id: id});
		if(!taxi){
			this.taxis.insert({
				id: id,
				longi: longi,
				lati: lati,
				type: 'location'
			})
			taxi = this.taxis.findOne({id:id});
		}else{
			taxi.longi = longi;
			taxi.lati = lati;
			this.taxis.update(taxi);
		}
		this.broadcast(JSON.stringify(taxi));	
	}

	updateCurrentNumTaxisDriving(val){
		this.broadcast(JSON.stringify({total: val, type: 'total'}));
	}

	updateOverallDistance(val){
		this.broadcast(JSON.stringify({distance: val, type: 'distance'}));
	}

	updateTaxiAreaViolation(id, isViolating){
		var taxi = this.taxis.findOne({id: id});
		if(taxi){
			taxi.areaViolation = isViolating;
			this.taxis.update(taxi);
		    this.broadcast(JSON.stringify(taxi));
		}
	}

	updateTaxiOutOfBounds(id, isOOB){
		var taxi = this.taxis.findOne({id: id});
		if(taxi){
		    taxi.areaViolation = isOOB;
		    this.taxis.update(taxi);
		    this.broadcast(JSON.stringify(taxi));
		}
	}

}


var server = new KafkaSocketServer();
server.listen();
