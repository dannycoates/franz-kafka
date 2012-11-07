var assert = require('assert')
var os = require('os')
var async = require('async')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var Stream = require('stream')

var noop = function () {}
var nullLogger = {}
Object.keys(console).forEach(function (f) { nullLogger[f] = noop })

function setLogger(logger) {
	if (logger) {
		var required = Object.keys(console)
		assert.ok(
			required.every(
				function (f) {
					return typeof(logger[f] === 'function')
				}
			),
			'logger must implement the global.console interface'
		)
		return logger
	}
	else {
		return nullLogger
	}
}

module.exports = function (options) {
	var logger = setLogger(options.logger)

	var Client = require('./client')(logger)
	var Partition = require('./partition')(logger)
	var Owner = require('./owner')(Partition)
	var Consumer = require('./consumer')(logger, async, os, inherits, EventEmitter, Owner)
	var Broker = require('./broker')(logger, inherits, EventEmitter, Client)
	var BrokerPool = require('./broker-pool')(logger, inherits, EventEmitter)
	var Producer = require('./producer')(inherits, EventEmitter, BrokerPool)
	var MessageBuffer = require('./message-buffer')()
	var Topic = require('./topic')(inherits, Stream, MessageBuffer)
	var StaticConnector = require('./static-connector')(logger, inherits, EventEmitter, Producer, Consumer, BrokerPool, Broker)

	if (options.zookeeper) {
		try {
			var ZooKeeper = require('zookeeper')
			var ZK = require('./zk')(logger, async, inherits, EventEmitter, ZooKeeper)
			var ZKConnector = require('./zkconnector')(logger, async, inherits, EventEmitter, ZK, Producer, Consumer, BrokerPool, Broker)
		}
		catch (e) {
			logger.error('node-zookeeper could not be loaded')
			throw e
		}
	}

	var Kafka = require('./kafka')(inherits, EventEmitter, Topic, ZKConnector, StaticConnector, Client.compression)
	return new Kafka(options)
}
