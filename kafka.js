module.exports = function (
	inherits,
	EventEmitter,
	Topic,
	ZKConnector,
	BrokerPool) {

	function Kafka(options) {
		this.topics = {}
		this.options = options || {}
		this.connector = null

	}
	inherits(Kafka, EventEmitter)

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
		if (typeof(onconnect) === 'function') {
			this.once('connect', onconnect)
		}
	}

	Kafka.prototype.connectConsumer = function (cb) {
		this.connector.connectConsumer(cb)
	}

	Kafka.prototype.topic = function (name) {
		var topic = this.topics[name] || new Topic(name, this)
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

	Kafka.prototype.publish = function (topic, messages) {
		this.connector.produce(topic, messages)
	}

	return Kafka
}
