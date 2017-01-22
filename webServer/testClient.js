var WebSocket = require('ws');
var ws = new WebSocket('ws://localhost:8080');
 
ws.on('open', function open() {
  ws.send('something');
});
 
ws.on('message', function(data, flags) {
	console.log(data)
	ws.send('heeeey')
  // flags.binary will be set if a binary data is received. 
  // flags.masked will be set if the data was masked. 
});
