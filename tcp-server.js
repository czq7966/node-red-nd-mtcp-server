'use strict';

module.exports = function (RED) {

    var socketTimeout = RED.settings.socketTimeout || null;

    function TcpServer(config) {

        var net = require('net'); //https://nodejs.org/api/net.html
        var crypto = require('crypto');

        RED.nodes.createNode(this, config);

        this.action = config.action || "listen"; /* listen,close,write */
        this.port = config.port * 1;
        this.topic = config.topic;
        this.stream = (!config.datamode || config.datamode=='stream'); /* stream,single*/
        this.datatype = config.datatype || 'buffer'; /* buffer,utf8,base64,xml */
        this.newline = (config.newline || "").replace("\\n","\n").replace("\\r","\r");

        var node = this;
        
        var connectionPool = {};
        var server;

        var findConnection = function(addr, port) {

            var id = null;

            for (var connId in connectionPool) {
                if (connectionPool.hasOwnProperty(connId)) {
                    if (connectionPool[connId].socket.remoteAddress == addr && connectionPool[connId].socket.remotePort == port) {
                        id = connId;
                        break;
                    }
                }
            }

            return id;
            
        };
		
        node.on('input', function (msg, nodeSend, nodeDone) {

            if (config.actionType === 'msg' || config.actionType === 'flow' || config.actionType === 'global') {
                node.action = RED.util.evaluateNodeProperty(config.action, config.actionType, this, msg);
            }

            console.log(node.action, msg);

            if (config.portType === 'msg' || config.portType === 'flow' || config.portType === 'global') {
                node.port = (RED.util.evaluateNodeProperty(config.port, config.portType, this, msg)) * 1;
            }

            var configure = (id) => {

                var socket = connectionPool[id].socket;

                socket.setKeepAlive(true, 120000);

                if (socketTimeout !== null) {
                    socket.setTimeout(socketTimeout);
                }

                socket.on('data', (data) => {

                    if (node.datatype != 'buffer') {
                        data = data.toString(node.datatype == 'xml' ? 'utf8' : node.datatype);
                    }
    
                    var buffer = connectionPool[id].buffer;
    
                    if (node.stream) {
    
                        var result = {
                            topic: msg.topic || config.topic,
                            _address: socket.remoteAddress,
                            _port: socket.remotePort,
                            _id: id
                        };
    
                        if ((typeof data) === "string" && node.newline !== "") {
    
                            buffer = buffer + data;
                            var parts = buffer.split(node.newline);
    
                            for (var i = 0; i < parts.length - 1; i += 1) {
                                
                                result.payload = parts[i];
    
                                if (node.datatype == 'xml') {
    
                                    var xml2js = require('xml2js');
                                    var parseXml = xml2js.parseString;
    
                                    var parseOpts = {
                                        async: true,
                                        attrkey: (config.xmlAttrkey || '$'),
                                        charkey: (config.xmlCharkey || '_'),
                                        explicitArray:  config.xmlArray,
                                        normalizeTags: config.xmlNormalizeTags,
                                        normalize: config.xmlNormalize
                                    };
    
                                    if (config.xmlStrip) {
                                        var stripPrefix = require('xml2js').processors.stripPrefix;
                                        parseOpts.tagNameProcessors = [ stripPrefix ];
                                        parseOpts.attrNameProcessors = [ stripPrefix ];
                                    }
    
                                    var parseStr = result.payload.replace(/^[\x00\s]*/g, ""); //Non-whitespace before first tag
                                    parseStr += node.newline;
    
                                    parseXml(parseStr, parseOpts, function (parseErr, parseResult) {
                                        if (!parseErr) { 
                                            result.payload = parseResult;
                                            nodeSend(result);
                                        }
                                    });
    
                                }
                                else {
                                    nodeSend(result);
                                }
    
                            }
    
                            buffer = parts[parts.length - 1];
    
                        }
                        else {
                            result.payload = data;
                            nodeSend(result);
                        }
    
                    }
                    else {
    
                        if ((typeof data) === "string") {
                            buffer = buffer + data;
                        }
                        else {
                            buffer = Buffer.concat([buffer, data], buffer.length + data.length);
                        }
    
                    }
    
                    connectionPool[id].buffer = buffer;

                });

                socket.on('end', function () {
                    if (!node.stream || (node.datatype === "utf8" && node.newline !== "")) {
                        var buffer = connectionPool[id].buffer;
                        if (buffer.length > 0) nodeSend({ topic: msg.topic || config.topic, payload: buffer, _address: socket.remoteAddress, _port: socket.remotePort, _id: id });
                        connectionPool[id].buffer = null;
                    }
                });

                socket.on('timeout', function () {
                    socket.end();
                });

                socket.on('close', function () {
                    delete connectionPool[id];
                });

                socket.on('error', function (err) {
                    node.log(err);
                });

            };

            var close = () => {

                var closeHost = node.closeHost;
                var closePort = node.closePort;

                if (config.closeHostType === 'msg' || config.closeHostType === 'flow' || config.closeHostType === 'global') {
                    closeHost = RED.util.evaluateNodeProperty(config.closeHost, config.closeHostType, this, msg);
                }

                if (config.closePortType === 'msg' || config.closePortType === 'flow' || config.closePortType === 'global') {
                    closePort = RED.util.evaluateNodeProperty(config.closePort, config.closePortType, this, msg);
                }

                if (closeHost && closePort) {

                    var closeId = findConnection(closeHost, closePort);

                    if (closeId) {
    
                        var socket = connectionPool[closeId].socket;
                        socket.end();
                        socket.destroy();
                        socket.unref();
    
                        delete connectionPool[closeId];
    
                    }

                } else {
                    for (var connId in connectionPool) {
                        if (connectionPool.hasOwnProperty(connId)) {
                            var socket = connectionPool[connId].socket;
                            socket.end();
                            socket.destroy();
                            socket.unref(); 
                            console.log(connId, socket.remoteAddress, socket.remotePort);

                        }
                        console.log(connId);
                    }

                    connectionPool = {};

                }
                
            };

            var write = () => {

                var writeMsg = config.write;

                if (config.writeType === 'msg' || config.writeType === 'flow' || config.writeType === 'global') {
                    writeMsg = RED.util.evaluateNodeProperty(config.write, config.writeType, this, msg);
                }



                if (writeMsg == null) return;

                var writeHost = node.writeHost;
                var writePort = node.closePort;

                if (config.writeHostType === 'msg' || config.writeHostType === 'flow' || config.writeHostType === 'global') {
                    writeHost = RED.util.evaluateNodeProperty(config.writeHost, config.writeHostType, this, msg);
                }

                if (config.writePortType === 'msg' || config.writePortType === 'flow' || config.writePortType === 'global') {
                    writePort = RED.util.evaluateNodeProperty(config.writePort, config.writePortType, this, msg);
                }

                if (writeHost && writePort) {

                    var writeId = findConnection(writeHost, writePort);

                    if (writeId) {
    
                        var socket = connectionPool[writeId].socket;
                   
                        if (Buffer.isBuffer(writeMsg)) {
                            socket.write(writeMsg);
                        } else if (typeof writeMsg === "string" && node.datatype == 'base64') {
                            socket.write(Buffer.from(writeMsg, 'base64'));
                        } else {
                            socket.write(Buffer.from("" + writeMsg));
                        }

                    }

                } else {
                    for (var connId in connectionPool) {
                        if (connectionPool.hasOwnProperty(connId)) {
                            var socket = connectionPool[connId].socket;
                            if (Buffer.isBuffer(writeMsg)) {
                                socket.write(writeMsg);
                            } else if (typeof writeMsg === "string" && node.datatype == 'base64') {
                                socket.write(Buffer.from(writeMsg, 'base64'));
                            } else {
                                socket.write(Buffer.from("" + writeMsg));
                            }                       
                        }
                    }                    
                }

            };

            var kill = () => {

                if (server) {

                    for (var connId in connectionPool) {
                        var socket = connectionPool[connId].socket;
                        socket.end();
                        socket.destroy();
                        socket.unref();
                    }
    
                    connectionPool = {};
                    server.close();

                }

            };

            var listen = () => {

                if (typeof server === 'undefined') {
    
                    server = net.createServer(function (socket) {

                        var id = crypto.createHash('md5').update(`${socket.localAddress}${socket.localPort}${socket.remoteAddress}${socket.remotePort}`).digest("hex");

                        connectionPool[id] = {
                            socket: socket,
                            buffer: (node.datatype == 'buffer') ? Buffer.alloc(0) : ""
                        };
                        
                        configure(id);
        
                    });
                    
                    server.on('error', function (err) {
                        if (err) node.error(err);
                    });
    
                }

                server.listen(node.port, function (err) {
                    if (err) node.error(err);
                    console.log("tcp server listin on port: ", node.port);
                });

            };
            
            if (node.action) {
                switch (node.action.toLowerCase()) {
                    case 'close':
                        close();
                        break;
                    case 'write':
                        write();
                        break;
                    case 'kill':
                        kill();
                        break;
                    default:
                        listen();
                }
            }

        });

        node.on("close",function() {
            if (server) {

                for (var connId in connectionPool) {
                    var socket = connectionPool[connId].socket;
                    socket.end();
                    socket.destroy();
                    socket.unref();
                }

                connectionPool = {};
                server.close();

            }            
            node.status({});
        });

    };

    RED.nodes.registerType("tcp-server", TcpServer);

};