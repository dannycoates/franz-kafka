var net = require('net')
var os = require('os')
var async = require('async')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var protocol = require('./protocol')
var ReadableStream = require('readable-stream')
var ZooKeeper = require('zookeeper')
var MessageBuffer = require('./message-buffer')()
var Topic = require('./topic')(inherits, EventEmitter, MessageBuffer)
var Client = require('./client')(
	net,
	inherits,
	EventEmitter,
	ReadableStream,
	protocol.Message,
	protocol.Receiver,
	protocol.FetchRequest,
	protocol.ProduceRequest,
	protocol.OffsetsRequest
)
var Broker = require('./broker')(inherits, EventEmitter, Client)
var BrokerPool = require('./broker-pool')(inherits, EventEmitter)
var Producer = require('./producer')(inherits, EventEmitter, BrokerPool)
var Consumer = require('./consumer')(os, inherits, EventEmitter)
var ZK = require('./zk')(async, inherits, EventEmitter, ZooKeeper)
var ZKConnector = require('./zkconnector')(async, inherits, EventEmitter, ZK, Producer, Consumer, BrokerPool, Broker)
var StaticConnector = require('./static-connector')(inherits, EventEmitter, Producer, Consumer, BrokerPool, Broker)
var kafka = require('./kafka')(inherits, EventEmitter, Topic, ZKConnector, StaticConnector, protocol.Message)

module.exports = kafka
