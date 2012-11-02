module.exports = function (
	inherits,
	EventEmitter,
	Topic,
	ZKConnector,
	StaticConnector,
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
		EventEmitter.call(this)
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
			this.connector = new StaticConnector(this.options)
		}
		this.connector.once(
			'brokerAdded', // TODO: create a more definitive event in the connectors
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
