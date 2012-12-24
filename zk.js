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
			hosts: options.zookeeper,
			logger: logger
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
				self.ensureRoots(self.emit.bind(self, 'connect'))
			}
		)
	}

	ZK.prototype.close = function () {
		this.zk.close()
	}

	ZK.prototype.ensureRoots = function (cb) {
		async.forEach(
			[
				'/brokers/ids',
				'/brokers/topics',
				'/consumers'
			],
			function (root, next) {
				this.zk.mkdirp(root, next)
			}.bind(this),
			cb
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
		logger.info('zk get topics')
		this.zk.getChildren(
			'/brokers/topics',
			this.subscribeToTopics.bind(this),
			this._topicsChanged.bind(this)
		)
	}

	ZK.prototype._topicsChanged = function (err, topics) {
		var self = this
		logger.info('zk topics', topics)
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
		logger.info('zk get', name, 'children')
		this.zk.getChildren(
			'/brokers/topics/' + name,
			this._getTopicBrokers.bind(this, name, noop),
			this._getBrokersPartitions.bind(this, name, done)
		)
	}

	ZK.prototype._getBrokersPartitions = function (name, done, err, brokerIds) {
		logger.info('zk topic', name, 'brokers', brokerIds)
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
		logger.info('zk get', name, 'broker', id)
		self.zk.get(
			'/brokers/topics/' + name + '/' + id,
			self._getPartitionCount.bind(self, name, id, noop),
			function (err, data, stat) {
				if (data) {
					var pcount = +(data.toString())
					logger.info('zk topic', name, 'broker', id, 'partitions', pcount)
					self.emit('broker-topic-partition', id, name, pcount)
				}
				done()
			}
		)
	}

	// TODO indeed
	//*

	ZK.prototype._createOrReplace = function (path, data, flags, cb) {
		var self = this
		async.waterfall([
			function (next) {
				self.zk.exists(path, next)
			},
			function (exists, stat, next) {
				if (exists) {
					self.zk.set(path, data, stat.version, next)
				}
				else {
					self.zk.create(path, data, flags, next)
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

	ZK.prototype.registerTopics = function (topics, kafka, cb) {
		var self = this
		logger.info('register', topics)
		async.series([
			function (next) {
				self._createConsumerRoots(kafka.groupId, next)
			},
			function (next) {
				self._createOrReplace(
					'/consumers/' + kafka.groupId + '/ids/' + kafka.consumerId,
					toTopicString(topics),
					self.zk.create.EPHEMERAL,
					next
				)
			}
			],
			function (err) {
				logger.info('registered')
				cb(err)
			}
		)
	}

	ZK.prototype.getTopicPartitions = function (topics, consumer, cb) {
		//TODO
		return cb(null, [])
		//return cb(null, [{topic: 'foo', partitions: ['0-0','0-1','1-0','1-1']}])
		//cb(null, [{topic: topics['bazzz'], partitions: ['0-0:0']}])
	}

	return ZK
}
