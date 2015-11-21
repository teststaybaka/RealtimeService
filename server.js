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

var server = http.createServer(function (request, response) {
    response.writeHead(200);
    response.end("Please connect with websocket.\n");
}).listen(80);

var video_clips = {};
var viewers_counter = {};
var Danmaku_Positions = {Scroll: 1, Top: 1, Bottom: 1};
var dataset = gcloud.datastore.dataset({
    projectId: 'dan-tube',
    keyFilename: './DanTube-88b03a33107c.json',
});

var url_req = /^\/(\d+)\?session=(.*)$/;
var wss = new WebSocketServer({server: server});
wss.on('connection', function(client) {
    var res = client.upgradeReq.url.match(url_req);
    if (!res) {
        client.close();
        return;
    }
    
    var clip_id = parseInt(res[1]);
    var parts = decodeURIComponent(res[2]).split('|');
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
        client.on('message', function(data) {
            data = querystring.parse(data);
            var user_id = session.user.user_id;
            var timestamp = parseFloat(data.timestamp);
            var size = parseInt(data.size);
            var color = parseInt(data.color, 16);
            if (isNaN(timestamp) || isNaN(size) || isNaN(color)) return;

            var position = data.type;
            if (!(position in Danmaku_Positions)) return;

            var content = data.content.trim();
            if (content.length === 0 || content.length > 350) return;

            var created = new Date();
            var year = created.getFullYear();
            var month = ('0' + (created.getMonth() + 1)).substr(-2);
            var date = ('0' + created.getDate()).substr(-2);
            var hours = ('0' + created.getHours()).substr(-2);
            var minutes = ('0' + created.getMinutes()).substr(-2);
            if (data.reply_to) {
                content = '←' + content;
            }
            
            var danmaku = {
                content: content,
                timestamp: timestamp,
                created: month+'-'+date+' '+hours+':'+minutes,
                created_year: year+'-'+month+'-'+date+' '+hours+':'+minutes,
                created_seconds: created.getTime()/1000,
                creator: user_id,
                type: position,
                size: size,
                color: color,
                // 'index': index,
            }
            broadcast_danmaku(clip_id, danmaku);
        });
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

