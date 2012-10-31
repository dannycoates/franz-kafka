module.exports = function (
	inherits,
	EventEmitter,
	Topic,
	ZKConnector,
	BrokerPool,
	Message) {

	/*
	 * options: {
	 *   zookeeper: 'address:port'
	 *   brokers:   [{name: host: port: },...]
	 *   compression: 'none', 'gzip', 'snappy'
	 *   maxMessageSize: -1
	 *   queueTime: 5000
	 *   batchSize: 200
	 * }
	 */
	function Kafka(options) {
		this.topics = {}
		this.options = options || {}
		this.connector = null
		this.queueTime = options.queueTime || 5000
		this.batchSize = options.batchSize || 200
		this.setCompression(options.compression)
	}
	inherits(Kafka, EventEmitter)

	Kafka.prototype.setCompression = function(string) {
		switch (string && string.toLowerCase()) {
			case 'gzip':
				this.compression = Message.compression.GZIP
				break;
			case 'snappy':
				this.compression = Message.compression.SNAPPY
				break;
			default:
				this.compression = Message.compression.NONE
				break;
		}
	}

	Kafka.prototype.connect = function (onconnect) {
		var self = this
		if (this.options.zookeeper) {
			this.connector = new ZKConnector(this.options)
		}
		else if (this.options.brokers) {
			this.connector = new BrokerPool()
			this.options.brokers.forEach(
				function (b) {
					var broker = new Broker(b.name, b.host, b.port)
					broker.once(
						'connect',
						function () {
							self.connector.add(broker)
						}
					)
				}
			)
		}
		this.connector.once(
			'brokerAdded',
			function () {
				self.emit('connect')
			}
		)
		this.connector.on(
			'brokerReady',
			function (b) {
				var topics = Object.keys(self.topics)
				for (var i = 0; i < topics.length; i++) {
					var name = topics[i]
					if (b.hasTopic(name)) {
						self.topics[name].setReady(true)
					}
				}
			}
		)
		if (typeof(onconnect) === 'function') {
			this.once('connect', onconnect)
		}
	}

	Kafka.prototype.connectConsumer = function (cb) {
		this.connector.connectConsumer(cb)
	}

	Kafka.prototype.topic = function (name) {
		var topic = this.topics[name] ||
			new Topic(
				name,
				this.connector,
				this.compression,
				this.batchSize,
				this.queueTime
			)
		this.topics[name] = topic
		return topic
	}

	Kafka.prototype.fetch = function (topic) {
		this.connector.fetch(topic)
	}

	Kafka.prototype.consume = function (name, interval) {
		var topic = this.topic(name)
		topic.consume(interval || 1000)
		return topic
	}

	Kafka.prototype.publish = function (name, messages) {
		this.topic(name).publish(messages)
	}

	return Kafka
}
