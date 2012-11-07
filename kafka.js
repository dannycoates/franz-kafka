module.exports = function (
	inherits,
	EventEmitter,
	Topic,
	ZKConnector,
	StaticConnector,
	Compression) {

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
				this.compression = Compression.GZIP
				break;
			case 'snappy':
				this.compression = Compression.SNAPPY
				break;
			default:
				this.compression = Compression.NONE
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

	Kafka.prototype.topic = function (name, options) {
		options = options || {}
		options.compression = options.compression || this.compression
		options.batchSize = options.batchSize || this.batchSize
		options.queueTime = options.queueTime || this.queueTime
		options.interval = options.interval || 1000
		var topic = this.topics[name] ||
			new Topic(
				name,
				this.connector.producer, //TODO
				this.connector.consumer,
				options
			)
		this.topics[name] = topic
		return topic
	}

	Kafka.prototype.publish = function (name, messages) {
		this.topic(name).publish(messages)
	}

	return Kafka
}
