var gcloud = require('gcloud');
var http = require('http');
var https = require('https');
var querystring = require('querystring');
var fs = require('fs');
var WebSocketServer = require('ws').Server;
var API_Key = 'AIzaSyBbf3cs6Nw483po40jw7hZLejmdrgwozWc';

var server = http.createServer(function (request, response) {
    response.writeHead(200);
    response.end("Please connect with websocket.\n");
}).listen(80);

var video_clips = {};
var peaks = {};
var dataset = gcloud.datastore.dataset({
    projectId: 'dan-tube',
    keyFilename: './DanTube-88b03a33107c.json',
});

var url_req = /^\/(\d+)$/;
var wss = new WebSocketServer({server: server});
wss.on('connection', function(client) {
    var res = client.upgradeReq.url.match(url_req);
    if (!res) {
        client.close();
        return;
    }

    var clip_id = parseInt(res[1]);
    if (!(clip_id in peaks)) {
        var peak_counter = [0, false];
        peaks[clip_id] = peak_counter;
        var key = dataset.key(['VideoClip', clip_id]);
        dataset.get(key, function(err, entity) {
            if (err || !entity) {
                console.log('Get video clip error, close websocket connection.');
                client.close();
            } else {
                console.log('Get video clip done');
                if (peak_counter[0] <= entity.data.peak) {
                    peak_counter[0] = entity.data.peak;
                    peak_counter[1] = false;
                }
                count_viewer(client, clip_id);
            }
        });
    } else {
        count_viewer(client, clip_id);
    }
});
wss.on('error', function(evt) {
    console.log('Websocket error: '+JSON.stringify(evt));
});

function count_viewer(client, clip_id) {
    if (!(clip_id in video_clips)) {
        video_clips[clip_id] = new LinkedClients();
    }
    var linkedClients = video_clips[clip_id];
    var peak_counter = peaks[clip_id];

    var clientNode = new ClientNode(client);
    linkedClients.push(clientNode);
    if (peak_counter[0] < linkedClients.length) {
        peak_counter[0] = linkedClients.length;
        peak_counter[1] = true;
    }

    console.log('Websocket connected: '+clip_id+' currently has '+linkedClients.length+' viewers.');
    client.send(JSON.stringify({
        type: 'viewers',
        current: linkedClients.length,
        peak: peak_counter[0]
    }));

    client.on('close', function() {
        console.log('One viewer closed from '+clip_id);
        linkedClients.remove(clientNode);
        if (linkedClients.length === 0) {
            delete video_clips[clip_id];
            if (!peak_counter[1]) return;
            
            console.log('Updating peak for video clip '+clip_id);
            var peak_value = peak_counter[0];
            var data = querystring.stringify({
                API_Key: API_Key,
                peak: peak_value,
            });
            var options = {
                hostname: 'dan-tube.appspot.com',
                path: '/video/update_peak/'+clip_id,
                method: 'POST',
                accept: '*/*',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Content-Length': Buffer.byteLength(data)
                }
            }
            var req = https.request(options, function(res) {
                res.setEncoding('utf8');
                res.on('data', function(data) {
                    data = JSON.parse(data);
                    if (data.error) {
                        console.log('Update peak error for video clip '+clip_id+': '+data.message);
                    } else {
                        if ((!clip_id in video_clips)) {
                            delete peaks[clip_id];
                        } else {
                            if (peak_counter[0] === peak_value) {
                                peak_counter[1] = false;
                            }
                        }
                    }
                });
            });
            req.write(data);
            req.end();

            req.on('error', function(err){
                console.log('HTTPS post error for video clip '+clip_id+':');
                console.log(err);
            });
        }
    });
}

(function broadcast_viewers() {
    console.log('Broadcasting current viewers');
    for (var clip_id in video_clips) {
        var linkedClients = video_clips[clip_id];
        var peak_counter = peaks[clip_id];
        var iterator = new LinkedListIterator(linkedClients);
        while (iterator.hasNext()) {
            var node = iterator.next();
            node.client.send(JSON.stringify({
                type: 'viewers',
                current: linkedClients.length,
                peak: peak_counter[0]
            }));
        }
    }
    setTimeout(broadcast_viewers, 60000);
})();

function ClientNode(client) {
    this.next = null;
    this.prev = null;
    this.client = client;
}

function LinkedClients() {
    this.head = new ClientNode(null);
    this.tail = new ClientNode(null);
    this.head.next = this.tail;
    this.tail.prev = this.head;
    this.length = 0;
}
LinkedClients.prototype.push = function(node) {
    node.next = this.tail;
    node.prev = this.tail.prev;
    this.tail.prev.next = node;
    this.tail.prev = node;
    this.length += 1;
}
LinkedClients.prototype.remove = function(node) {
    node.prev.next = node.next;
    node.next.prev = node.prev;
    this.length -= 1;
}

function LinkedListIterator(linkedClients) {
    this.pointer = linkedClients.head;
    this.end = linkedClients.tail;
}
LinkedListIterator.prototype.hasNext = function() {
    return this.pointer.next !== this.end;
}
LinkedListIterator.prototype.next = function() {
    if (this.pointer.next === this.end) {
        throw StopIteration;
    } else {
        this.pointer = this.pointer.next;
        return this.pointer;
    }
}

