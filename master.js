var child_process = require('child_process');
var events = require('events');

function MasterNode() {}
MasterNode.prototype = new events.EventEmitter;
MasterNode.prototype.startServer = function() {
    console.log('Starting server...');
    var server = child_process.fork('./server.js');

    server.on('close', function (code) {
        console.log('Server exited with code '+code);
        this.emit('close', code);
    });
}

var master = new MasterNode();
master.startServer();
master.on('close', function(code) {
   master.startServer(); 
});

