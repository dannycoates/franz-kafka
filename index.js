var net = require('net')
var async = require('async')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var protocol = require('./protocol')
var ReadableStream = require('readable-stream')
var ZooKeeper = require('zookeeper')
var Topic = require('./topic')(inherits, EventEmitter)
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
var BrokerPool = require('./broker-pool.js')(inherits, EventEmitter)
var ZKConnector = require('./zkconnector')(async, inherits, EventEmitter, ZooKeeper, BrokerPool, Broker)

var kafka = require('./kafka')(inherits, EventEmitter, Topic, ZKConnector, BrokerPool)

module.exports = kafka
