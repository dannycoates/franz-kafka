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
			this.connector = new ZKConnector(this.options.zookeeper)
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

	Kafka.prototype.createTopic = function (name) {
		var t = new Topic(name, this)
		this.topics[name] = t
		return t
	}

	Kafka.prototype.consume = function (topic, interval) {
		//TODO: a better interval method
		var self = this
		clearInterval(topic.interval)
		setInterval(
			function () {
				self.connector.fetch(topic)
			},
			interval
		)
	}

	Kafka.prototype.publish = function (topic, messages) {
		this.connector.produce(topic, messages)
	}

	return Kafka
}
