module.exports = function (
	os,
	async,
	inherits,
	EventEmitter,
	ZooKeeper,
	BrokerPool,
	Broker
	) {

	function noop() {}

	function genConsumerId(groupId) {
		return groupId + '_' + os.hostname() + '-' + Date.now() + '-' + "DEADBEEF"
	}

	// options: {
	//   zookeeper:
	//   groupId:
	// }
	function ZKConnector(options) {
		var self = this
		this.zk = new ZooKeeper({
			connect: options.zookeeper,
			timeout: 200000,
 			debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARNING,
 			host_order_deterministic: false,
		})
		this.zk.once(
			'close',
			function () { console.log('zk close')}
		)
		this.brokerPool = new BrokerPool()
		this.brokerPool.on(
			'brokerAdded',
			function (b) {
				console.log('added ' + b.id)
				self.emit('brokerAdded', b)
			}
		)
		this.brokerPool.on(
			'brokerRemoved',
			function (b) {
				console.log('removed ' + b.id)
				self.emit('brokerRemoved', b)
			}
		)
		this.brokerPool.on(
			'brokerReady',
			function (b) {
				self.emit('brokerReady', b)
			}
		)
		this.groupId = options.groupId
		this.consumerId = genConsumerId(this.groupId)
		this.connect()
	}
	inherits(ZKConnector, EventEmitter)

	ZKConnector.prototype.connect = function () {
		var self = this
		this.zk.connect(
			function (err) {
				if (err) {
					return self.emit('error', err)
				}
				self._getBrokers()
				self._getTopics()
			}
		)
	}

	ZKConnector.prototype._getBrokers = function () {
		this.zk.aw_get_children(
			'/brokers/ids',
			this._getBrokers.bind(this),
			this._brokersChanged.bind(this)
		)
	}

	ZKConnector.prototype._brokersChanged = function (rc, err, brokerIds) {
		var self = this
		if (brokerIds) {
			async.forEachSeries(
				brokerIds,
				function (id, next) {
					if (!self.brokerPool.contains(id)) {
						self._addBroker(id, next)
					}
				},
				function (err) {
					self.brokerPool.removeBrokersNotIn(brokerIds)
				}
			)
		}
	}

	ZKConnector.prototype._addBroker = function (id, done) {
		var self = this
		this.zk.a_get(
			'/brokers/ids/' + id,
			false,
			function (rc, err, stat, data) {
				if (data) {
					self._createBroker(id, data.toString())
					done()
				}
			}
		)
	}

	ZKConnector.prototype._createBroker = function (id, info) {
		var self = this
		var split = info.split(':')
		if (split.length > 2) {
			var broker = new Broker(id, split[1], split[2])
			broker.once(
				'connect',
				function () {
					self.brokerPool.add(broker)
				}
			)
		}
	}

	ZKConnector.prototype._getTopics = function () {
		this.zk.aw_get_children(
			'/brokers/topics',
			this._getTopics.bind(this),
			this._topicsChanged.bind(this)
		)
	}

	ZKConnector.prototype._topicsChanged = function (rc, err, topics) {
		var self = this
		if (topics) {
			async.forEachSeries(
				topics,
				function (topic, next) {
					self._getTopicBrokers(topic, next)
				},
				function (err) {

				}
			)
		}
	}

	ZKConnector.prototype._getTopicBrokers = function (name, done) {
		this.zk.aw_get_children(
			'/brokers/topics/' + name,
			this._getTopicBrokers.bind(this, name, noop),
			this._getBrokerTopicPartitionCount.bind(this, name, done)
		)
	}

	ZKConnector.prototype._getBrokerTopicPartitionCount = function (name, done, rc, err, brokerIds) {
		var self = this
		if (brokerIds) {
			async.forEachSeries(
				brokerIds,
				function (id, next) {
					self._setBrokerTopicPartitionCount(name, id, next)
				},
				function (err) {
					done()
				}
			)
		}
	}

	ZKConnector.prototype._setBrokerTopicPartitionCount = function (name, id, done) {
		var self = this
		self.zk.aw_get(
			'/brokers/topics/' + name + '/' + id,
			self._setBrokerTopicPartitionCount.bind(self, name, id, noop),
			function (rc, err, stat, data) {
				if (data) {
					self.brokerPool.setBrokerTopicPartitionCount(
						id,
						name,
						+(data.toString())
					)
				}
				done()
			}
		)
	}

	ZKConnector.prototype.connectConsumer = function (cb) {

	}

	ZKConnector.prototype.fetch = function (topic) {

	}

	ZKConnector.prototype.produce = function (topic, messages) {
		return this.brokerPool.produce(topic, messages)
	}

	return ZKConnector
}
