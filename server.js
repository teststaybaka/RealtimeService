var gcloud = require('gcloud');
var http = require('http');
var https = require('https');
var querystring = require('querystring');
var fs = require('fs');
var WebSocketServer = require('ws').Server;
var crypto = require('crypto');
var algorithm = 'sha256';
var cookie_name = 'db_session';
var secret_key = 'Mfrghtrrouhsmvnmxdiosjkgjfds68_=iooijgrdxuihbvc97yutcivbhugd409k';
var API_Key = 'AIzaSyBbf3cs6Nw483po40jw7hZLejmdrgwozWc';

var api_req = /^\/(\d+)\?entry=(.*)$/;
var server = http.createServer(function (request, response) {
    var res = request.url.match(api_req);
    if (res) {
        try {
            var clip_id = parseInt(res[1]);
            var parts = decodeURIComponent(res[2]).split('|');
            if (parts.length !== 2) {
                throw 'Invalid params';
            }

            var signature = crypto.createHmac(algorithm, secret_key)
                                    .update(parts[0])
                                    .digest('hex');
            if (parts[1] !== signature) {
                throw 'Invalid signature';
            }

            response.writeHead(200);
            response.end("Danmaku received.\n");

            var danmaku = JSON.parse(new Buffer(parts[0], 'base64').toString('utf8'));
            broadcast_danmaku(clip_id, danmaku);
        } catch (err) {
            console.log(err);
            response.writeHead(200);
            response.end("Please connect with websocket.\n");
        }
    } else {
        response.writeHead(200);
        response.end("Please connect with websocket.\n");
    }
}).listen(80);

var video_clips = {};
var viewers_counter = {};
var view_timestamps = {};
var dataset = gcloud.datastore.dataset({
    projectId: 'dan-tube',
    keyFilename: './DanTube-88b03a33107c.json',
});

var url_req = /^\/(dt\d+)\/(\d+)\?session=(.*)&index=(\d+)$/;
var wss = new WebSocketServer({server: server});
wss.on('connection', function(client) {
    var res = client.upgradeReq.url.match(url_req);
    if (!res) {
        client.close();
        return;
    }
    
    var video_id = res[1];
    var clip_id = parseInt(res[2]);
    var clip_index = parseInt(res[4]);
    var parts = decodeURIComponent(res[3]).split('|');
    if (parts.length !== 3) {
        client.close();
        return;
    }

    var signature = crypto.createHmac(algorithm, secret_key)
                            .update(cookie_name+'|'+parts[0]+'|'+parts[1])
                            .digest('hex');
    if (parts[2] !== signature) {
        console.log('Invalid signature');
        client.close();
        return;
    }
    var session = JSON.parse(new Buffer(parts[0], 'base64').toString('utf8'));

    if (clip_id in viewers_counter) {
        count_viewers(client, clip_id);
    } else {
        var key = dataset.key(['VideoClip', clip_id]);
        dataset.get(key, function(err, entity) {
            if (err || !entity) {
                console.log('Get video clip error, close websocket connection.');
                client.close();
            } else {
                if (clip_id in viewers_counter) {
                    var viewer_counter = viewers_counter[clip_id];
                    if (viewer_counter[0] <= entity.data.peak) {
                        viewer_counter[0] = entity.data.peak;
                        viewer_counter[1] = 0;
                    }
                } else {
                    viewers_counter[clip_id] = [entity.data.peak, 0, 0, 0, 0];
                }
                count_viewers(client, clip_id);
            }
        });
    }

    if (session.user) {
        var user_id = session.user.user_id;
        var viewrecord_key = video_id+'v:'+user_id;
        if (viewrecord_key in view_timestamps) {
            timestamp_timer(client, viewrecord_key, clip_id, clip_index, user_id);
        } else {
            var key = dataset.key(['ViewRecord', viewrecord_key]);
            dataset.get(key, function(err, entity) {
                if (err || !entity) {
                    console.log('Get view record error.');
                } else {
                    if (!(viewrecord_key in view_timestamps)) {
                        view_timestamps[viewrecord_key] = [entity.data.clip_index, entity.data.timestamp, 0, 0, 0];
                    }
                    timestamp_timer(client, viewrecord_key, clip_id, clip_index, user_id);
                }
            });
        }
    }
});
wss.on('error', function(evt) {
    console.log('Websocket error: '+JSON.stringify(evt));
});

function count_viewers(client, clip_id) {
    var viewer_counter = viewers_counter[clip_id];
    var linkedClients;
    if (!(clip_id in video_clips)) {
        linkedClients = new LinkedClients();
        video_clips[clip_id] = linkedClients;
    } else {
        linkedClients = video_clips[clip_id];
    }
    var clientNode = new ClientNode(client);
    linkedClients.push(clientNode);

    viewer_counter[3] += 1;
    viewer_counter[4] = 1;
    if (viewer_counter[0] < viewer_counter[3]) {
        viewer_counter[0] = viewer_counter[3];
        viewer_counter[1] = 1;
    }

    // console.log('Websocket connected: '+clip_id+' currently has '+linkedClients.length+' viewers.');
    client.send(JSON.stringify({
        type: 'viewers',
        current: viewer_counter[3],
        peak: viewer_counter[0]
    }));
    
    client.on('close', function() {
        linkedClients.remove(clientNode);
        viewer_counter[3] -= 1;
        viewer_counter[4] = 1;
        if (linkedClients.length === 0) {
            delete video_clips[clip_id];
            flush_peak(clip_id, viewer_counter);    
        }
    });
}

function flush_peak(clip_id, viewer_counter) {
    if (viewer_counter[1] === 0) {
        delete viewers_counter[clip_id];
        return;
    }
    if (viewer_counter[2] === 1) {
        console.log('Peak value is updating');
        return;
    }

    console.log('Updating peak for video clip '+clip_id+':'+viewer_counter[0]+' '+viewer_counter[1]);
    viewer_counter[1] = 0;
    viewer_counter[2] = 1;
    var data = querystring.stringify({
        API_Key: API_Key,
        peak: viewer_counter[0],
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
            viewer_counter[2] = 0;
            if (data.error) {
                console.log('Update peak error for video clip '+clip_id+': '+data.message);
            } else {
                if (!(clip_id in video_clips)) {
                    if (viewer_counter[1] === 1) {
                        flush_peak(clip_id, viewer_counter);
                    } else {
                        delete viewers_counter[clip_id];
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
        viewer_counter[2] = 0;
    });
}

function timestamp_timer(client, viewrecord_key, clip_id, clip_index, user_id) {
    var timestamp_state = view_timestamps[viewrecord_key];
    timestamp_state[2] += 1;
    if (timestamp_state[0] !== clip_index) {
        timestamp_state[0] = clip_index;
        timestamp_state[1] = 0;
    }

    client.send(JSON.stringify({
        type: 'timestamp',
        timestamp: timestamp_state[1]
    }));

    client.on('message', function(timestamp) {
        timestamp = parseFloat(timestamp);
        if (isNaN(timestamp)) return;

        timestamp_state[1] = timestamp;
        timestamp_state[3] = 1;
    });

    client.on('close', function() {
        timestamp_state[2] -= 1;
        if (timestamp_state[2] === 0) {
            update_view_history(clip_id, user_id, viewrecord_key, timestamp_state);
        }
    });
}

function update_view_history(clip_id, user_id, viewrecord_key, timestamp_state) {
    if (timestamp_state[3] === 0) {
        delete view_timestamps[viewrecord_key];
        return;
    }
    if (timestamp_state[4] === 1) {
        console.log('Timestamp value is updating');
        return;
    }

    timestamp_state[3] = 0;
    timestamp_state[4] = 1;
    var data = querystring.stringify({
        API_Key: API_Key,
        user_id: user_id,
        timestamp: timestamp_state[1]
    });
    var options = {
        hostname: 'dan-tube.appspot.com',
        path: '/video/update_history/'+clip_id,
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
            timestamp_state[4] = 0;
            if (data.error) {
                console.log('Update view history error for video clip '+clip_id+': '+data.message);
            } else {
                if (timestamp_state[2] === 0) {
                    if (timestamp_state[3] === 1) {
                        update_view_history(clip_id, user_id, viewrecord_key, timestamp_state);
                    } else {
                        delete view_timestamps[viewrecord_key];
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
        timestamp_state[4] = 0;
    });
}

function broadcast_danmaku(clip_id, danmaku) {
    console.log('Broadcasting danmaku');
    var linkedClients = video_clips[clip_id];
    var iterator = new LinkedListIterator(linkedClients);
    var message = JSON.stringify({
                                    type: 'danmaku',
                                    entry: danmaku
                                });

    while (iterator.hasNext()) {
        var node = iterator.next();
        node.client.send(message);
    }
}

(function broadcast_viewers() {
    console.log('Broadcasting viewers');
    for (var clip_id in video_clips) {
        var viewer_counter = viewers_counter[clip_id];
        if (viewer_counter[4] === 1) {
            var linkedClients = video_clips[clip_id];
            delayed_broadcast_viewers(linkedClients, viewer_counter);
        }
    }
    setTimeout(broadcast_viewers, 60000);
})();

function delayed_broadcast_viewers(linkedClients, viewer_counter) {
    process.nextTick(function() {
        viewer_counter[4] = 0;
        var iterator = new LinkedListIterator(linkedClients);
        var message = JSON.stringify({
                                        type: 'viewers',
                                        current: viewer_counter[3],
                                        peak: viewer_counter[0]
                                    });

        while (iterator.hasNext()) {
            var node = iterator.next();
            node.client.send(message);
        }
    });
}

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

