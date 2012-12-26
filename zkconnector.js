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
		this.zk = new ZK(kafka.groupId, kafka.consumerId, options)
		this.hasPendingTopics = false
		this.isConsuming = false
		this.rebalancing = false
		this.interestedTopics = {}
		this.rebalance = rebalance.bind(this)
		this.registerTopics = registerTopics.bind(this)
		this.onBrokerConnect = brokerConnect.bind(this)
		this.onBrokersChanged = brokersChanged.bind(this)
		this.onBrokerTopicPartition = setPartitionCount.bind(this)
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
		this.zk.on('brokers', this.onBrokersChanged)
		this.zk.on('broker-topic-partition', this.onBrokerTopicPartition)
		this.zk.on('consumers', this.rebalance)
	}

	ZKConnector.prototype.close = function () {
		this.zk.removeListener('brokers', this.onBrokersChanged)
		this.zk.removeListener('broker-topic-partition', this.onBrokerTopicPartition)
		this.zk.removeListener('consumers', this.rebalance)
		this.zk.close()
	}

	function topicObject(name) { return this.interestedTopics[name] }

	ZKConnector.prototype._topics = function () {
		return Object.keys(this.interestedTopics).map(topicObject.bind(this))
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

	ZKConnector.prototype._removeBrokersNotIn = function (brokerIds) {
		var brokers = this.brokers.all()
		for (var i = 0; i < brokers.length; i++) {
			var broker = brokers[i]
			if (brokerIds.indexOf(broker.id) === -1) {
				this.brokers.remove(broker)
			}
		}
	}

	ZKConnector.prototype.register = function (topic) {
		//return false // TODO enable when rebalance works
		if (!this.interestedTopics[topic.name]) {
			this.hasPendingTopics = true
			this.interestedTopics[topic.name] = topic
			process.nextTick(this.registerTopics)
		}
	}

	function rebalance() {
		if (this.rebalancing) {
			return
		}
		logger.info('rebalancing')
		this.rebalancing = true
		async.waterfall([
			function (next) {
				async.forEachSeries(
					this._topics(),
					function (topic, done) {
						logger.info('draining', topic.name)
						topic.resetConsumer(done)
					},
					function (err) {
						logger.info('drained')
						next()
					})
			}.bind(this),
			function (next) {
				this.zk.releasePartitionOwnership(next)
			}.bind(this),
			function (next) {
				this.zk.getTopicPartitions(this.interestedTopics, this.kafka, next)
			}.bind(this),
			function (topicPartitions, next) {
				var topicNames = Object.keys(topicPartitions)
				for(var i = 0; i < topicNames.length; i++) {
					var name = topicNames[i]
					var partitions = topicPartitions[name]
					var topic = this.kafka.topic(name)
					logger.info(
						'consume', name,
						'partitions', partitions
					)
					topic.addReadablePartitions(partitions)
					topic.resume()
				}
				this.rebalancing = false
				next()
			}.bind(this)
		])
	}

	function registerTopics() {
		if (!this.isConsuming) {
			this.isConsuming = true
			return this.zk.registerTopicsAndSubscribe(this.interestedTopics)
		}
		if (this.hasPendingTopics) {
			var self = this
			this.zk.registerTopics(
				this.interestedTopics,
				function () {
					self.rebalance()
				}
			)
			this.hasPendingTopics = false
		}
	}

	function brokerConnect(broker) {
		this.brokers.add(broker)
	}

	function brokersChanged(brokerIds) {
		var self = this
		async.forEachSeries(
			brokerIds,
			function (id, next) {
				if (!self.brokers.get(id)) {
					self.zk.getBroker(id, self._createBroker.bind(self))
				}
				next()
			},
			function (err) {
				self._removeBrokersNotIn(brokerIds)
			}
		)
	}

	function setPartitionCount(brokerId, topicName, count) {
		var topic = this.kafka.topic(topicName)
		topic.addWritablePartitions([brokerId + ':' + count])
		// if (this.isConsuming) {
		// 	this.rebalance()
		// }
	}

	return ZKConnector
}
