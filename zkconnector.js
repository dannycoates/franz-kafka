module.exports = function (
	async,
	inherits,
	EventEmitter,
	ZooKeeper,
	BrokerPool,
	Broker
	) {

	function noop() {}

	function ZKConnector(connect) {
		var self = this
		this.zk = new ZooKeeper({
			connect: connect,
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
		this.topicPartitions = {}
		this.groupId = "foo"
		this.consumerId = "bar"
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
				self._getBrokers(
					function () {
						self._getTopics()
					}
				)
			}
		)
	}

	ZKConnector.prototype._getBrokers = function (done) {
		this.zk.aw_get_children(
			'/brokers/ids',
			this._getBrokers.bind(this, this._getTopics.bind(this)),
			this._brokersChanged.bind(this, done)
		)
	}

	ZKConnector.prototype._brokersChanged = function (done, rc, err, brokerIds) {
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
					done()
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
		this.zk.a_get_children(
			'/brokers/topics/' + name,
			false,
			this._getBrokerTopicPartitionCount.bind(this, name, done)
		)
	}

	ZKConnector.prototype._getBrokerTopicPartitionCount = function (name, done, rc, err, brokerIds) {
		var self = this
		if (brokerIds) {
			async.forEachSeries(
				brokerIds,
				function (id, next) {
					self.zk.a_get(
						'/brokers/topics/' + name + '/' + id,
						false,
						function (rc, err, stat, data) {
							if (data) {
								self.brokerPool.setBrokerTopicPartitionCount(
									id,
									name,
									+(data.toString())
								)
							}
							next()
						}
					)
				},
				function (err) {
					done()
				}
			)
		}
	}

	ZKConnector.prototype.fetch = function (topic) {

	}

	ZKConnector.prototype.produce = function (topic, messages) {
		this.brokerPool.produce(topic, messages)
	}

	return ZKConnector
}
