var net = require('net')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var protocol = require('./protocol')
var ReadableStream = require('readable-stream')
var Client = require('./client')(
	net,
	inherits,
	EventEmitter,
	ReadableStream,
	protocol.Receiver,
	protocol.FetchRequest,
	protocol.ProduceRequest)

module.exports = Client
