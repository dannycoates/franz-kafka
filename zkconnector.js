module.exports = function (
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

	// options: {
	//   zookeeper:
	//   groupId:
	// }
	function ZKConnector(options) {
		var self = this
		this.zk = new ZK(options)
		this.allBrokers = new BrokerPool()
		this.producer = new Producer(this.allBrokers)
		this.allBrokers.on(
			'brokerAdded',
			function (b) {
				console.log('added ' + b.id)
				self.emit('brokerAdded', b)
			}
		)
		this.allBrokers.on(
			'brokerRemoved',
			function (b) {
				console.log('removed ' + b.id)
				self.emit('brokerRemoved', b)
			}
		)
		this.brokerReady = function () {
			self.emit('brokerReady', this)
		}
		this.pendingConsumers = []
		this.registerConsumers = registerConsumers.bind(this)
		this.consumer = new Consumer(options.groupId, this.allBrokers)
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
			var broker = new Broker(id, split[1], split[2])
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

	}

	function registerConsumers() {
		if (this.pendingConsumers.length > 0) {
			var self = this
			this.consumer.foo(this.pendingConsumers) //TODO name me
			this.zk.registerConsumers(
				this.consumer,
				function (topicPartitions) {
					for(var i = 0; i < topicPartitions.length; i++) {
						var tp = topicPartitions[i]
						self.consumer.consume(tp.topic, tp.interval, tp.partitions)
					}
				}
			)
			this.pendingConsumers = []
		}
	}

	ZKConnector.prototype.consume = function (topic, interval) {
		this.pendingConsumers.push({topic: topic, interval: interval})
		process.nextTick(this.registerConsumers)
	}

	ZKConnector.prototype.publish = function (topic, messages) {
		return this.producer.publish(topic, messages)
	}

	return ZKConnector
}
