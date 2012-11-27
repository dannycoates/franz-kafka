module.exports = function (
	logger,
	async,
	inherits,
	EventEmitter,
	ZK,
	Broker
	) {

	function noop() {}

	// ZooKeeper is a disgusting parasite that has unfortunately attached
	// itself to poor Kafka.
	//
	// ZKConnector attempts to isolate the infestation from the rest of the code.
	// It manages communication with the beast, and handles brokers and
	// consumers, topic partitions, and offsets.
	//
	// options: {
	//   zookeeper:
	//   groupId:
	// }
	function ZKConnector(kafka, brokers, options) {
		this.kafka = kafka
		this.brokers = brokers
		this.options = options
		this.zk = new ZK(options)
		this.hasPendingTopics = false
		this.interestedTopics = {}
		this.registerTopics = registerTopics.bind(this)
		this.onBrokerConnect = brokerConnect.bind(this)
		this.connect()
		EventEmitter.call(this)
	}
	inherits(ZKConnector, EventEmitter)

	ZKConnector.prototype.connect = function () {
		var self = this
		this.zk.connect()
		this.zk.once('connect', function () {
			self.zk.subscribeToBrokers()
			self.zk.subscribeToTopics()
		})
		this.zk.on('brokers', this._brokersChanged.bind(this))
		this.zk.on('broker-topic-partition', this._setPartitionCount.bind(this))
		this.zk.on('consumers-changed', this._rebalance.bind(this))
	}

	function topicObject(name) { return this.interestedTopics[name] }

	ZKConnector.prototype._topics = function () {
		return Object.keys(this.interestedTopics).map(topicObject.bind(this))
	}

	ZKConnector.prototype._brokersChanged = function (brokerIds) {
		var self = this
		async.forEachSeries(
			brokerIds,
			function (id, next) {
				if (!self.brokers.get(id)) {
					self.zk.getBroker(id, self._createBroker.bind(self))
				}
			},
			function (err) {
				self._removeBrokersNotIn(brokerIds)
			}
		)
	}

	ZKConnector.prototype._createBroker = function (id, info) {
		var hostPort = info.split(':')
		if (hostPort.length > 2) {
			var host = hostPort[1]
			var port = hostPort[2]
			var oldBroker = this.brokers.get(id)
			if (oldBroker) {
				if (oldBroker.host === host && oldBroker.port === port) {
					return
				}
				else {
					this.brokers.remove(oldBroker)
				}
			}
			var broker = new Broker(id, { host: host, port: port })
			broker.once('connect', this.onBrokerConnect)
			broker.connect()
		}
	}

	ZKConnector.prototype._setPartitionCount = function (brokerId, topicName, count) {
		var topic = this.kafka.topic(topicName)
		topic.addWritablePartitions([brokerId + ':' + count])
	}

	ZKConnector.prototype._removeBrokersNotIn = function (brokerIds) {
		var brokers = this.brokers.all()
		for (var i = 0; i < brokers.length; i++) {
			var broker = brokers[i]
			if (brokerIds.indexOf(broker.id) === -1) {
				this.brokers.remove(broker)
			}
		}
	}

	ZKConnector.prototype._rebalance = function () {
		logger.info('rebalancing')
		async.waterfall([
			function (next) {
				async.forEach(
					this._topics(),
					function (topic, done) {
						topic.stop()
						topic.drain(done)
					},
					function (err) {
						next()
					})
			}.bind(this),
			function (next) {
				self.zk.getTopicPartitions(self.interestedTopics, self.consumer, next)
			},
			function (topicPartitions) {
				for(var i = 0; i < topicPartitions.length; i++) {
					var tp = topicPartitions[i]
					self.consumer.consume(tp.topic, tp.partitions)
				}
			}
		])
	}

	function registerTopics() {
		if (this.hasPendingTopics) {
			var self = this
			this.zk.registerTopics(
				this.interestedTopics,
				this.consumer,
				function () {
					self._rebalance()
				}
			)
			this.hasPendingTopics = false
		}
	}

	ZKConnector.prototype.register = function (topic) {
		return false // TODO enable when rebalance works
		if (!this.interestedTopics[topic.name]) {
			this.hasPendingTopics = true
			this.interestedTopics[topic.name] = topic
			process.nextTick(this.registerTopics)
		}
	}

	function brokerConnect(broker) {
		this.brokers.add(broker)
	}

	return ZKConnector
}
