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
			logger.log(
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
					logger.log('broker connected', self.id)
					self.emit('connect')
				}
			)
			this.client.once(
				'end',
				function () {
					//TODO: smarter reconnect
					logger.log('broker ended', self.id)
					self.connect()
				}
			)
			this.client.on(
				'ready',
				function () {
					logger.log('broker ready', self.id)
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
		logger.log(
			'set broker partitions', this.id,
			'topic', name,
			'partitions', count
		)
		this.topicPartitions[name] = new TopicPartition(name, count)
	}

	Broker.prototype.clearTopicPartitions = function () {
		logger.log('clear broker partitions', this.id)
		this.topicPartitions = {}
	}

	Broker.prototype.fetch = function (topic, partition, maxSize, cb) {
		this.client.fetch(topic, partition, maxSize, cb)
	}

	Broker.prototype.publish = function (topic, messages, cb) {
		var partition = 0
		var tp = this.topicPartitions[topic.name]
		if (tp) {
			partition = tp.next()
		}
		return this.client.publish(topic, messages, partition, cb)
	}

	Broker.prototype.drain = function (cb) {
		this.client.drain(cb)
	}

	return Broker
}
