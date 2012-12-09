module.exports = function (
	logger,
	async,
	inherits,
	EventEmitter,
	ZooKeeper) {

	function noop() {}

	// A feable attempt a wrangling the horrible ZooKeeper API
	function ZK(options) {
		this.zk = new ZooKeeper({
			hosts: options.zookeeper
		})
		this.zk.once(
			'expired',
			function () { logger.info('zk expired') }
		)
		EventEmitter.call(this)
	}
	inherits(ZK, EventEmitter)

	ZK.prototype.connect = function () {
		var self = this
		this.zk.start(
			function (err) {
				if (err) {
					return self.emit('error', err)
				}
				self.emit('connect')
			}
		)
	}

	ZK.prototype.subscribeToBrokers = function () {
		this.zk.getChildren(
			'/brokers/ids',
			this.subscribeToBrokers.bind(this),
			this._brokersChanged.bind(this)
		)
	}

	ZK.prototype._brokersChanged = function (err, brokerIds) {
		if (brokerIds) {
			this.emit('brokers', brokerIds)
		}
	}

	ZK.prototype.getBroker = function (id, done) {
		var self = this
		this.zk.get(
			'/brokers/ids/' + id,
			function (err, data) {
				var str = data ? data.toString() : ''
				done(id, str)
			}
		)
	}

	ZK.prototype.subscribeToTopics = function () {
		this.zk.getChildren(
			'/brokers/topics',
			this.subscribeToTopics.bind(this),
			this._topicsChanged.bind(this)
		)
	}

	ZK.prototype._topicsChanged = function (err, topics) {
		var self = this
		async.forEachSeries(
			topics || [],
			function (topic, next) {
				self._getTopicBrokers(topic, next)
			},
			function (err) {

			}
		)
	}

	ZK.prototype._getTopicBrokers = function (name, done) {
		this.zk.getChildren(
			'/brokers/topics/' + name,
			this._getTopicBrokers.bind(this, name, noop),
			this._getBrokersPartitions.bind(this, name, done)
		)
	}

	ZK.prototype._getBrokersPartitions = function (name, done, err, brokerIds) {
		var self = this
		if (brokerIds) {
			async.forEachSeries(
				brokerIds || [],
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
		self.zk.get(
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

	// TODO indeed
	/*
	ZK.prototype._create = function (path, data, options, cb) {
		this.zk.create(path, data, options,
			function (err, stat) {
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
						logger.info('exists', path, 'stat', stat)
						next(err, stat)
					}
				)
			},
			function (stat, next) {
				if (stat) {
					self.zk.a_set(path, data, stat.version,
						function (rc, err, stat) {
							logger.info('set', path, 'stat', stat)
							next(err, stat)
						}
					)
				}
				else {
					self.zk.a_create(path, data, options,
						function (rc, err, stat) {
							logger.info('create', path, 'stat', stat)
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
					logger.error('create consumer roots', err)
				}
				logger.info('created roots')
				cb(err)
			}
		)
	}

	function toTopicString(topics) {
		var names = Object.keys(topics)
		var ts = {}
		for (var i = 0; i < names.length; i++) {
			ts[names[i]] = 1
		}
		return JSON.stringify(ts)
	}

	ZK.prototype.registerTopics = function (topics, consumer, cb) {
		var self = this
		logger.info('registerTopics')
		async.series([
			function (next) {
				self._createConsumerRoots(consumer.groupId, next)
			},
			function (next) {
				self._createOrReplace(
					'/consumers/' + consumer.groupId + '/ids/' + consumer.consumerId,
					toTopicString(topics),
					ZooKeeper.ZOO_EPHEMERAL,
					next
				)
			}
			],
			function (err) {
				logger.info('registeredTopics')
				cb(err)
			}
		)
	}
	*/
	ZK.prototype.getTopicPartitions = function (topics, consumer, cb) {
		//TODO
		throw new Error("Not Implemented")
		cb(null, [{topic: topics['bazzz'], partitions: ['0-0:0']}])
	}

	return ZK
}
