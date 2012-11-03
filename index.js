var net = require('net')
var os = require('os')
var async = require('async')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var ZooKeeper = require('zookeeper')

var Client = require('./client')
var Partition = require('./partition')()
var Owner = require('./owner')(Partition)
var Consumer = require('./consumer')(async, os, inherits, EventEmitter, Owner)
var Broker = require('./broker')(inherits, EventEmitter, Client)
var BrokerPool = require('./broker-pool')(inherits, EventEmitter)
var Producer = require('./producer')(inherits, EventEmitter, BrokerPool)
var StaticConnector = require('./static-connector')(inherits, EventEmitter, Producer, Consumer, BrokerPool, Broker)
var ZK = require('./zk')(async, inherits, EventEmitter, ZooKeeper)
var ZKConnector = require('./zkconnector')(async, inherits, EventEmitter, ZK, Producer, Consumer, BrokerPool, Broker)
var MessageBuffer = require('./message-buffer')()
var Topic = require('./topic')(inherits, EventEmitter, MessageBuffer)
var kafka = require('./kafka')(inherits, EventEmitter, Topic, ZKConnector, StaticConnector, Client.compression)

module.exports = kafka
