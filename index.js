var net = require('net')
var os = require('os')
var async = require('async')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var Client = require('./protocol')

var ZooKeeper = require('zookeeper')
var MessageBuffer = require('./message-buffer')()
var Topic = require('./topic')(inherits, EventEmitter, MessageBuffer)

var Partition = require('./partition')()
var Owner = require('./owner')(Partition)
var Broker = require('./broker')(inherits, EventEmitter, Client)
var BrokerPool = require('./broker-pool')(inherits, EventEmitter)
var Producer = require('./producer')(inherits, EventEmitter, BrokerPool)
var Consumer = require('./consumer')(async, os, inherits, EventEmitter, Owner)
var ZK = require('./zk')(async, inherits, EventEmitter, ZooKeeper)
var ZKConnector = require('./zkconnector')(async, inherits, EventEmitter, ZK, Producer, Consumer, BrokerPool, Broker)
var StaticConnector = require('./static-connector')(inherits, EventEmitter, Producer, Consumer, BrokerPool, Broker)
var kafka = require('./kafka')(inherits, EventEmitter, Topic, ZKConnector, StaticConnector, Client.compression)

module.exports = kafka
