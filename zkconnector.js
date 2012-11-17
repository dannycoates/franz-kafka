module.exports = function (
	logger,
	async,
	inherits,
	EventEmitter,
	ZK,
	Producer,
	Consumer,
	BrokerPool,
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
	function ZKConnector(options) {
		var self = this
		this.options = options
		this.zk = new ZK(options)
		this.allBrokers = new BrokerPool('all')
		this.producer = new Producer(this.allBrokers)
		this.allBrokers.on(
			'brokerAdded',
			function (b) {
				self.emit('brokerAdded', b)
			}
		)
		this.allBrokers.on(
			'brokerRemoved',
			function (b) {
				self.emit('brokerRemoved', b)
			}
		)
		this.brokerReady = function () {
			self.emit('brokerReady', this)
		}
		this.hasPendingTopics = false
		this.interestedTopics = {}
		this.registerTopics = registerTopics.bind(this)
		this.consumer = new Consumer(this, options.groupId, this.allBrokers)
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
		this.zk.on('broker-topic-partition', this._setBrokerTopicPartitionCount.bind(this))
		this.zk.on('consumers-changed', this._rebalance.bind(this))
	}

	ZKConnector.prototype._brokersChanged = function (brokerIds) {
		var self = this
		async.forEachSeries(
			brokerIds,
			function (id, next) {
				if (!self.allBrokers.contains(id)) {
					self.zk.getBroker(id, self._createBroker.bind(self))
				}
			},
			function (err) {
				self.producer.removeBrokersNotIn(brokerIds)
			}
		)
	}

	ZKConnector.prototype._createBroker = function (id, info) {
		var self = this
		var split = info.split(':')
		if (split.length > 2) {
			var broker = new Broker(id, { host: split[1], port: split[2]})
			broker.on('ready', this.brokerReady)
			broker.once(
				'connect',
				function () {
					self.allBrokers.add(broker)
				}
			)
		}
	}

	ZKConnector.prototype._setBrokerTopicPartitionCount = function (broker, topic, count) {
		this.producer.setBrokerTopicPartitionCount(broker, topic, count)
	}

	ZKConnector.prototype._rebalance = function () {
		var self = this
		logger.info('rebalancing')
		async.waterfall([
			function (next) {
				self.consumer.stop()
				self.consumer.drain(next)
			},
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

	ZKConnector.prototype.consume = function (topic) {
		this.hasPendingTopics = true
		this.interestedTopics[topic.name] = topic
		process.nextTick(this.registerTopics)
	}

	ZKConnector.prototype.saveOffset = function (partition) {
		logger.info(
			'saving', partition.id,
			'broker', partition.broker.id,
			'offset', partition.offset
		)
		//TODO implement
	}

	return ZKConnector
}
