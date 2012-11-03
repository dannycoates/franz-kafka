module.exports = function (
	async,
	inherits,
	EventEmitter,
	ZooKeeper) {

	function noop() {}

	function ZK(options) {
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
		EventEmitter.call(this)
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
				var str = data ? data.toString() : ''
				done(id, str)
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

	ZK.prototype._create = function (path, data, options, cb) {
		this.zk.a_create(path, data, options,
			function (rc, err, stat) {
				switch (rc) {
					case ZooKeeper.ZOK:
						cb(null, stat)
						break;
					case ZooKeeper.ZNODEEXISTS:
						cb(null, path)
						break;
					default:
						cb(new Error(rc))
						break;
				}
			}
		)
	}

	ZK.prototype._createOrReplace = function (path, data, options, cb) {
		var self = this
		async.waterfall([
			function (next) {
				self.zk.a_exists(path, false,
					function (rc, err, stat) {
						next(err, stat)
					}
				)
			},
			function (stat, next) {
				if (stat) {
					self.zk.a_set(path, data, stat.version,
						function (rc, err, stat) {
							next(err, stat)
						}
					)
				}
				else {
					self.zk.a_create(path, data, options,
						function (rc, err, stat) {
							next(err, stat)
						}
					)
				}
			}
			],
			function (err, result) {
				cb(err)
			}
		)
	}

	ZK.prototype._createConsumerRoots = function (groupId, cb) {
		var self = this
		var base = '/consumers/' + groupId
		var roots = [base + '/ids', base + '/owners', base + '/offsets']
		async.forEachSeries(
			roots,
			function (root, next) {
				self.zk.mkdirp(root, next)
			},
			function (err) {
				if (err) {
					console.log(err)
				}
				cb(err)
			}
		)
	}

	ZK.prototype.registerTopics = function (topics, consumer, cb) {
		var self = this
		async.series([
			function (next) {
				self._createConsumerRoots(consumer.groupId, next)
			},
			function (next) {
				self._createOrReplace(
					'/consumers/' + consumer.groupId + '/ids/' + consumer.consumerId,
					JSON.stringify(topics),
					ZooKeeper.ZOO_EPHEMERAL,
					next
				)
			}
			],
			function (err) {
				cb(err)
			}
		)
	}

	ZK.prototype.getTopicPartitions = function (topics, consumer, cb) {
		//TODO
		cb([{topic: 'bar', interval: 200, partitions: ['0-0']}])
	}

	return ZK
}
