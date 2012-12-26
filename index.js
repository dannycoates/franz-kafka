var assert = require('assert')
var os = require('os')
var async = require('async')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var path = require('path')
var Stream = require('stream')

var RingList = require('./ring-list')()

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
	var Consumer = require('./consumer')(logger, inherits, EventEmitter, RingList)
	var Producer = require('./producer')(logger, inherits, RingList)
	var Broker = require('./broker')(logger, inherits, EventEmitter, Client)
	var BrokerPool = require('./broker-pool')(logger, inherits, EventEmitter)
	var Partition = require('./partition')(logger, inherits, EventEmitter, Broker)
	var PartitionSet = require('./partition-set')(logger, inherits, EventEmitter, Consumer, Producer)
	var MessageBuffer = require('./message-buffer')(inherits, EventEmitter)
	var Topic = require('./topic')(logger, inherits, Stream, MessageBuffer, Partition, PartitionSet)
	var StaticConnector = require('./static-connector')(logger, inherits, EventEmitter, Broker)

	if (options.zookeeper) {
		try {
			var ZooKeeper = require('zkjs')
			var ZK = require('./zk')(logger, async, inherits, EventEmitter, path, ZooKeeper)
			var ZKConnector = require('./zkconnector')(logger, async, inherits, EventEmitter, ZK, Broker)
		}
		catch (e) {
			logger.error('zkjs could not be loaded')
			throw e
		}
	}

	var Kafka = require('./kafka')(inherits, EventEmitter, os, BrokerPool, Topic, ZKConnector, StaticConnector, Client.compression)
	return new Kafka(options)
}
