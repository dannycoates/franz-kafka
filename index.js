var os = require('os')
var async = require('async')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var ZooKeeper = require('zookeeper')
var logger = console

var Client = require('./client')
var Partition = require('./partition')(logger)
var Owner = require('./owner')(Partition)
var Consumer = require('./consumer')(logger, async, os, inherits, EventEmitter, Owner)
var Broker = require('./broker')(inherits, EventEmitter, Client)
var BrokerPool = require('./broker-pool')(inherits, EventEmitter)
var Producer = require('./producer')(inherits, EventEmitter, BrokerPool)
var StaticConnector = require('./static-connector')(logger, inherits, EventEmitter, Producer, Consumer, BrokerPool, Broker)
var ZK = require('./zk')(logger, async, inherits, EventEmitter, ZooKeeper)
var ZKConnector = require('./zkconnector')(logger, async, inherits, EventEmitter, ZK, Producer, Consumer, BrokerPool, Broker)
var MessageBuffer = require('./message-buffer')()
var Topic = require('./topic')(inherits, EventEmitter, MessageBuffer)
var kafka = require('./kafka')(inherits, EventEmitter, Topic, ZKConnector, StaticConnector, Client.compression)

module.exports = kafka
