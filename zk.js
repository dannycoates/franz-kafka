module.exports = function (
	logger,
	async,
	inherits,
	EventEmitter,
	pathUtil,
	ZooKeeper) {

	function noop() {}

	// A feable attempt a wrangling the horrible ZooKeeper API
	function ZK(groupId, consumerId, options) {
		EventEmitter.call(this)
		this.zk = new ZooKeeper({
			hosts: options.zookeeper,
			logger: logger
		})
		this.zk.once(
			'expired',
			function () { logger.info('zk expired') }
		)
		this.groupId = groupId
		this.consumerId = consumerId
		this.claimAttemptsLeft = 5
		this.ownedPartitions = {}
		this.topics = {}
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

	ZK.prototype._subscribeToConsumerGroup = function () {
		this.zk.getChildren(
			'/consumers/' + this.groupId + '/ids',
			this._subscribeToConsumerGroup.bind(this),
			this._consumersChanged.bind(this)
		)
	}

	ZK.prototype.registerTopicsAndSubscribe = function(topics) {
		async.series([
			function (next) {
				this._createConsumerRoots(next)
			}.bind(this),
			function (next) {
				this.registerTopics(topics, next)
			}.bind(this),
			function (next) {
				this._subscribeToConsumerGroup()
				next()
			}.bind(this)
		])
	}

	ZK.prototype._consumersChanged = function (err, consumerIds) {
		logger.info('consumers', consumerIds)
		if (consumerIds) {
			this.emit('consumers', consumerIds)
		}
	}

	ZK.prototype.subscribeToBrokers = function () {
		this.zk.getChildren(
			'/brokers/ids',
			this.subscribeToBrokers.bind(this),
			this._brokersChanged.bind(this)
		)
	}

	ZK.prototype._brokersChanged = function (err, brokerIds) {
		logger.info('brokers', brokerIds)
		if (brokerIds) {
			this.emit('brokers', brokerIds)
		}
	}

	ZK.prototype.getBroker = function (id, done) {
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
		logger.info('zk topics', topics)
		async.forEachSeries(
			topics,
			function (topic, next) {
				this._getTopicBrokers(topic, next)
			}.bind(this),
			noop
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
		if (brokerIds) {
			async.forEachSeries(
				brokerIds,
				function (id, next) {
					this._getPartitionCount(name, id, next)
				}.bind(this),
				done
			)
		}
	}

	function partitionIds(brokerId, partitionCount) {
		var paritionIds = []
		for (var i = 0; i < partitionCount; i++) {
			paritionIds.push(brokerId + '-' + i)
		}
		return paritionIds
	}

	ZK.prototype._getPartitionCount = function (name, id, done) {
		logger.info('zk get', name, 'broker', id)
		this.zk.get(
			'/brokers/topics/' + name + '/' + id,
			this._getPartitionCount.bind(this, name, id, noop),
			function (err, data, stat) {
				if (data) {
					var pcount = +(data.toString())
					logger.info('zk topic', name, 'broker', id, 'partitions', pcount)
					this.topics[name] = this.topics[name] || []
					this.topics[name] = this.topics[name].concat(partitionIds(id, pcount)).sort()
					this.emit('broker-topic-partition', id, name, pcount)
				}
				done()
			}.bind(this)
		)
	}

	ZK.prototype._createOrReplace = function (path, data, flags, cb) {
		async.waterfall([
			function (next) {
				this.zk.mkdirp(pathUtil.dirname(path), next)
			}.bind(this),
			function (p, next) {
				logger.info('checking', path)
				this.zk.exists(path, next)
			}.bind(this),
			function (exists, stat, next) {
				if (exists) {
					logger.info(
						'setting', path,
						'data', data
					)
					this.zk.set(path, data, stat.version, next)
				}
				else {
					logger.info(
						'creating', path,
						'data', data
					)
					this.zk.create(path, data, flags, next)
				}
			}.bind(this)
			],
			function (err, result) {
				logger.info('create/replace', path)
				cb(err)
			}
		)
	}

	ZK.prototype._createConsumerRoots = function (cb) {
		var self = this
		var base = '/consumers/' + this.groupId
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

	ZK.prototype.rmrf = function (path, cb) {
		this.zk.getChildren(
			path,
			function (err, children, stat) {
				if (err) {
					return cb(err)
				}
				if (children.length === 0) {
					logger.info('deleting', path)
					return this.zk.del(path, stat.version, cb)
				}
				async.forEachSeries(
					children,
					function (child, next) {
						this.rmrf(path + '/' + child, next)
					}.bind(this),
					cb
				)
			}.bind(this)
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

	ZK.prototype.registerTopics = function (topics, cb) {
		var topicString = toTopicString(topics)
		logger.info('register', topicString)
		this._createOrReplace(
			'/consumers/' + this.groupId + '/ids/' + this.consumerId,
			topicString,
			this.zk.create.EPHEMERAL,
			cb
		)
	}

	ZK.prototype.releasePartitionOwnership = function (cb) {
		var topicNames = Object.keys(this.ownedPartitions)
		async.forEachSeries(
			topicNames,
			function (name, next) {
				async.forEachSeries(
					this.ownedPartitions[name],
					function (partitionName, next) {
						var path = '/consumers/' + this.groupId + '/owners/' + name + '/' + partitionName
						logger.info('removing', path)
						this.rmrf(path, next)
					}.bind(this),
					next
				)
			}.bind(this),
			function (err) {
				logger.info('released partitions')
				this.ownedPartitions = {}
				cb()
			}.bind(this)
		)
	}

	function parseTopicPattern(topics, pattern) {
		var first = pattern[0]
		var topicCounts = {}
		switch (first) {
			case '{':
				topicCounts = JSON.parse(pattern)
				break;
			case '*':
				var parts = pattern.split('*')
				var count = +(parts[1])
				var regex = new RegExp(parts[2])
				topics.forEach(
					function (topic) {
						if (regex.test(topic)) {
							topicCounts[topic] = count
						}
					}
				)
				break;
			case '!':
				var parts = pattern.split('!')
				var count = +(parts[1])
				var regex = new RegExp(parts[2])
				topics.forEach(
					function (topic) {
						if (!regex.test(topic)) {
							topicCounts[topic] = count
						}
					}
				)
				break;
		}
		return topicCounts
	}

	ZK.prototype.getConsumers = function (cb) {
		logger.info('get consumers')
		var topics = Object.keys(this.topics)
		this.zk.getChildren(
			'/consumers/' + this.groupId + '/ids',
			function (err, children, stat) {
				if (err) {
					return cb(err)
				}
				async.mapSeries(
					children,
					function (id, next) {
						this.zk.get(
							'/consumers/' + this.groupId + '/ids/' + id,
							function (err, x, stat) {
								if (err) {
									return next(err)
								}
								next(
									null,
									{
										id: id,
										topics: parseTopicPattern(topics, x.toString('utf8'))
									}
								)
							}
						)
					}.bind(this),
					function (err, results) {
						var result = {}
						if (results) {
							results.forEach(
								function (r) {
									result[r.id] = r.topics
								}
							)
						}
						logger.info('consumers', result)
						cb(err, result)
					}
				)
			}.bind(this)
		)
	}

	function copyTopicPartitions(original) {
		var copy = {}
		var names = Object.keys(original)
		for (var i = 0; i < names.length; i++) {
			var name = names[i]
			copy[name] = original[name].concat()
		}
		return copy
	}

	ZK.prototype.calculatePartitions = function (consumerTopicCounts) {
		var topics = copyTopicPartitions(this.topics)
		var consumerTopicPartitions = {}
		var consumerIds = Object.keys(consumerTopicCounts).sort()
		var consumerCount = consumerIds.length
		var topicNames = Object.keys(topics).sort()
		logger.info(
			'calculate partitions',
			'consumers', consumerIds,
			'topics', topicNames
		)
		for (var i = 0; i < consumerIds.length; i++) {
			var consumerId = consumerIds[i]
			var consumer = consumerTopicCounts[consumerId]
			consumerTopicPartitions[consumerId] = {}

			for (var j = 0; j < topicNames.length; j++) {
				var topicName = topicNames[j]
				var consumerTopicCount = consumer[topicName] || 0

				if (consumerTopicCount > 0) {
					var partitions = topics[topicName]
					var partitionCount = partitions.length
					var partsPerConsumer = partitionCount / consumerCount
					var extras = partitionCount % consumerCount
					var myCount = partsPerConsumer + (i + 1 > extras ? 0 : 1)
					var owned = []

					for (var k = 0; k < myCount; k++) {
						owned.push(partitions.shift())
					}
					logger.info(
						'consumer',consumerId,
						'topic', topicName,
						'claims', owned
					)
					consumerTopicPartitions[consumerId][topicName] = owned
				}
			}
		}

		return consumerTopicPartitions[this.consumerId]
	}

	ZK.prototype.claimPartitions = function (topicPartitions, cb) {
		if (!this.claimAttemptsLeft) {
			this.claimAttemptsLeft = 5
			return cb(new Error('exhausted partition claim attempts'))
		}
		this.claimAttemptsLeft--
		var topicNames = Object.keys(topicPartitions)
		async.forEachSeries(
			topicNames,
			function (topicName, next) {
				async.forEachSeries(
					topicPartitions[topicName],
					function (partition, next) {
						logger.info('claiming', topicName, partition)
						this._createOrReplace(
							'/consumers/' + this.groupId + '/owners/' + topicName + '/' + partition,
							this.consumerId,
							this.zk.create.EPHEMERAL,
							next
						)
					}.bind(this),
					next
				)
			}.bind(this),
			function (err) {
				if (!err) {
					this.claimAttemptsLeft = 5
					this.ownedPartitions = topicPartitions
					logger.info('claimed', topicPartitions)
					cb(null, topicPartitions)
				}
				else {
					logger.info('claiming partitions failed', err)
					this.claimPartitions(topicPartitions, cb)
				}
			}.bind(this)
		)
	}

	ZK.prototype.getTopicPartitions = function (topics, consumer, cb) {
		this.getConsumers(
			function (err, consumerTopicCounts) {
				if (err) {
					return cb(err)
				}
				this.claimPartitions(this.calculatePartitions(consumerTopicCounts), cb)
			}.bind(this)
		)
	}

	return ZK
}
