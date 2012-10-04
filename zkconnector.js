module.exports = function (
	inherits,
	EventEmitter,
	ZooKeeper,
	BrokerPool,
	Broker
	) {

	function ZKConnector(connect) {
		var self = this
		this.zk = new ZooKeeper({
			connect: connect,
			timeout: 200000,
 			debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARNING,
 			host_order_deterministic: false,
		})
		this.brokerPool = new BrokerPool()
		this.brokerPool.on(
			'brokerAdded',
			function (b) {
				self.emit('brokerAdded', b)
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
				self.getTopicPartitions()
				self.getBrokers()
			}
		)
	}

	ZKConnector.prototype.getBrokers = function () {
		this.zk.a_get_children('/brokers/ids', true, this.brokersChanged.bind(this))
	}

	ZKConnector.prototype.brokersChanged = function (rc, err, brokerIds) {
		if (brokerIds) {
			brokerIds = brokerIds.map(function (id) { return +id })
			for (var i = 0; i < brokerIds.length; i++) {
				var id = brokerIds[i]
				if (!this.brokerPool.contains(id)) {
					this.addBroker(id)
				}
			}
			this.brokerPool.removeBrokersNotIn(brokerIds)
		}
	}

	ZKConnector.prototype.addBroker = function (id) {
		var self = this
		this.zk.a_get(
			'/brokers/ids/' + id,
			false,
			function (rc, err, stat, data) {
				if (data) {
					self.createBroker(id, data.toString())
				}
			}
		)
	}

	ZKConnector.prototype.createBroker = function (id, info) {
		var self = this
		var split = info.split(':')
		if (split.length > 2) {
			var broker = new Broker(id, split[1], +(split[2]))
			broker.once(
				'connect',
				function () {
					self.brokerPool.add(broker)
				}
			)
		}
	}

	ZKConnector.prototype.getTopicPartitions = function () {
		this.zk.a_get_children('/brokers/topics', true, this.topicPartitionsChanged.bind(this))
	}

	ZKConnector.prototype.topicPartitionsChanged = function (rc, err, topics) {
		if (topics) {
			//TODO it
		}
	}

	ZKConnector.prototype.fetch = function (topic) {

	}

	ZKConnector.prototype.produce = function (topic, messages) {
		this.brokerPool.produce(topic, messages)
	}

	return ZKConnector
}
