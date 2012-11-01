module.exports = function (
	async,
	inherits,
	EventEmitter,
	ZooKeeper) {

	function noop() {}

	function ZK(connect) {
		this.zk = new ZooKeeper({
			connect: options.zookeeper,
			timeout: 200000,
			debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARNING,
			host_order_deterministic: false
		})
		this.zk.once(
			'close',
			function () { console.log('zk close')}
		)
		this.consumer = {}
	}
	inherits(ZK, EventEmitter)

	ZK.prototype.connect = function () {
		var self = this
		this.zk.connect(
			function (err) {
				if (err) {
					return self.emit('error', err)
				}
				self.emit('connect')
			}
		)
	}

	ZK.prototype.subscribeToBrokers = function () {
		this.zk.aw_get_children(
			'/brokers/ids',
			this.subscribeToBrokers.bind(this),
			this._brokersChanged.bind(this)
		)
	}

	ZK.prototype._brokersChanged = function (rc, err, brokerIds) {
		if (brokerIds) {
			this.emit('brokers', brokerIds)
		}
	}

	ZK.prototype.getBroker = function (id, done) {
		var self = this
		this.zk.a_get(
			'/brokers/ids/' + id,
			false,
			function (rc, err, stat, data) {
				done(id, data || '')
			}
		)
	}

	ZK.prototype.subscribeToTopics = function () {
		this.zk.aw_get_children(
			'/brokers/topics',
			this.subscribeToTopics.bind(this),
			this._topicsChanged.bind(this)
		)
	}

	ZK.prototype._topicsChanged = function (rc, err, topics) {
		var self = this
		async.forEachSeries(
			topics,
			function (topic, next) {
				self._getTopicBrokers(topic, next)
			},
			function (err) {

			}
		)
	}

	ZK.prototype._getTopicBrokers = function (name, done) {
		this.zk.aw_get_children(
			'/brokers/topics/' + name,
			this._getTopicBrokers.bind(this, name, noop),
			this._getBrokersPartitions.bind(this, name, done)
		)
	}

	ZK.prototype._getBrokersPartitions = function (name, done, rc, err, brokerIds) {
		var self = this
		if (brokerIds) {
			async.forEachSeries(
				brokerIds,
				function (id, next) {
					self._getPartitionCount(name, id, next)
				},
				function (err) {
					done()
				}
			)
		}
	}

	ZK.prototype._getPartitionCount = function (name, id, done) {
		var self = this
		self.zk.aw_get(
			'/brokers/topics/' + name + '/' + id,
			self._getPartitionCount.bind(self, name, id, noop),
			function (rc, err, stat, data) {
				if (data) {
					self.emit('broker-topic-partition', id, name, +(data.toString()))
				}
				done()
			}
		)
	}

	ZK.prototype.registerConsumer = function (topic, cb) {
		var self = this
		this.consumer[topic.name] = 1
		this.zk.a_create(
			'/consumers/' + this.groupId + '/ids/' + this.consumerId,
			JSON.stringify(this.consumer),
			ZooKeeper.ZOO_EPHEMERAL,
			function (rc, err, path) {

			}
		)
	}

	return ZK
}
