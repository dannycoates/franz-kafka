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

	function Broker(id, host, port, options) {
		this.id = id
		this.topicPartitions = {}
		this.client = null
		this.reconnectAttempts = 0
		options = options || {}
		options.host = host
		options.port = port
		this.connect(options)
		EventEmitter.call(this)
	}
	inherits(Broker, EventEmitter)

	function exponentialBackoff(attempt) {
		return Math.floor(
			Math.random() * Math.pow(2, attempt) * 10
		)
	}

	Broker.prototype.connect = function (options) {
		var self = this
		logger.info(
			'connecting broker', self.id,
			'host', options.host,
			'port', options.port
		)
		this.client = new Client(this.id, options)
		this.client.once(
			'connect',
			function () {
				logger.info('broker connected', self.id)
				self.reconnectAttempts = 0
				self.emit('connect')

			}
		)
		this.client.once(
			'end',
			function () {
				self.reconnectAttempts++
				logger.info('broker ended', self.id, self.reconnectAttempts)
				setTimeout(
					function () {
						self.connect(options)
					},
					exponentialBackoff(self.reconnectAttempts)
				)
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

	Broker.prototype.write = function (topic, messages, cb) {
		var partitionId = 0
		var tp = this.topicPartitions[topic.name]
		if (tp) {
			partitionId = tp.next()
		}
		return this.client.write(topic, messages, partitionId, cb)
	}

	Broker.prototype.drain = function (cb) {
		this.client.drain(cb)
	}

	return Broker
}
