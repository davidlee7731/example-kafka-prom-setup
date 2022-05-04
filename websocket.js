import WebSocket from 'ws'
import express from 'express'
import client from 'prom-client'

function WebSocketClient() {
    this.number = 0;	// Message number
    this.autoReconnectInterval = 5 * 1000;	// ms
}
WebSocketClient.prototype.open = function (url) {
    this.url = url;
    this.instance = new WebSocket(this.url);
    this.instance.on('open', () => {
        this.onopen();
    });
    this.instance.on('message', (data, flags) => {
        this.number++;
        this.onmessage(data, flags, this.number);
    });
    this.instance.on('close', (e) => {
        switch (e.code) {
            case 1000:	// CLOSE_NORMAL
                console.log("WebSocket: closed");
                break;
            default:	// Abnormal closure
                this.reconnect(e);
                break;
        }
        this.onclose(e);
    });
    this.instance.on('error', (e) => {
        this.onerror(e);
        switch (e.code) {
            case 'ECONNREFUSED':
                this.reconnect(e);
                break;
        }
    });
}
WebSocketClient.prototype.send = function (data, option) {
    try {
        this.instance.send(data, option);
    } catch (e) {
        this.instance.emit('error', e);
    }
}
WebSocketClient.prototype.reconnect = function (e) {
    console.log(`WebSocketClient: retry in ${this.autoReconnectInterval}ms`, e);
    this.instance.removeAllListeners();
    var that = this;
    setTimeout(function () {
        console.log("WebSocketClient: reconnecting...");
        that.open(that.url);
    }, this.autoReconnectInterval);
}
WebSocketClient.prototype.onopen = function (...e) { console.log("WebSocketClient: open", ...e); }
WebSocketClient.prototype.onmessage = function (data, flags, number) { console.log("WebSocketClient: message", arguments); }
WebSocketClient.prototype.onerror = function (...e) { console.log("WebSocketClient: error", ...e); }
WebSocketClient.prototype.onclose = function (...e) { console.log("WebSocketClient: closed", ...e); }

const ENDPOINT = process.env.ENDPOINT ?? 'ws://kafka:7071'

const app = express()

const gauge = new client.Gauge({
  name: 'websocket',
  help: 'websocket_help',
  labelNames: ['status']
});

const ws = new WebSocketClient();
ws.open(ENDPOINT);
ws.onopen = function (e) {
  gauge.labels('status').set(1);
}
ws.onerror = function (e) {
  gauge.labels('status').set(0);
}

app.get('/metrics', (req, res) => {
  res.end(client.register.metrics());
});

app.listen(9189);