module.exports = function (
	logger,
	inherits,
	EventEmitter,
	Client) {

	function TopicPartition(name, count) {
		this.name = name
		this.count = count
		this.current = 0
	}

	TopicPartition.prototype.next = function () {
		this.current = (this.current + 1) % this.count
		return this.current
	}

	function Broker(id, host, port) {
		this.id = id
		this.topicPartitions = {}
		this.client = null
		this.connect(host, port)
		EventEmitter.call(this)
	}
	inherits(Broker, EventEmitter)

	Broker.prototype.connect = function (host, port) {
		var self = this
		if (!this.client) {
			logger.info(
				'connecting broker', self.id,
				'host', host,
				'port', port
			)
			this.client = new Client(
				this.id,
				{
					host: host,
					port: port
				}
			)
			this.client.once(
				'connect',
				function () {
					logger.info('broker connected', self.id)
					self.emit('connect')
				}
			)
			this.client.once(
				'end',
				function () {
					//TODO: smarter reconnect
					logger.info('broker ended', self.id)
					self.connect()
				}
			)
			this.client.on(
				'ready',
				function () {
					logger.info('broker ready', self.id)
					self.emit('ready')
				}
			)
		}
	}

	Broker.prototype.isReady = function () {
		return this.client.ready
	}

	Broker.prototype.hasTopic = function (name) {
		return !!this.topicPartitions[name]
	}

	Broker.prototype.setTopicPartitions = function (name, count) {
		logger.info(
			'set broker partitions', this.id,
			'topic', name,
			'partitions', count
		)
		this.topicPartitions[name] = new TopicPartition(name, count)
	}

	Broker.prototype.clearTopicPartitions = function () {
		logger.info('clear broker partitions', this.id)
		this.topicPartitions = {}
	}

	Broker.prototype.fetch = function (name, partition, maxSize, cb) {
		this.client.fetch(name, partition, maxSize, cb)
	}

	Broker.prototype.publish = function (topic, messages, cb) {
		var partitionId = 0
		var tp = this.topicPartitions[topic.name]
		if (tp) {
			partitionId = tp.next()
		}
		return this.client.publish(topic, messages, partitionId, cb)
	}

	Broker.prototype.drain = function (cb) {
		this.client.drain(cb)
	}

	return Broker
}
