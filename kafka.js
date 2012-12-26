module.exports = function (
	inherits,
	EventEmitter,
	os,
	BrokerPool,
	Topic,
	ZKConnector,
	StaticConnector,
	Compression) {

	// Kafka is the cornerstone of any nutritious queuing strategy.
	// With it you can connect to a Kafka cluster and create
	// Topics to your heart's content.
	//
	// options: {
	//   zookeeper: 'address:port',
	//   brokers:   [{id: host: port: },...],
	//   compression: 'none',
	//   maxMessageSize: 1000000,
	//   queueTime: 5000,
	//   batchSize: 200,
	//   groupId: 'franz-kafka',
	//   minFetchDelay: 0,
	//   maxFetchDelay: 10000,
	//   maxFetchSize: 300*1024,
	//   logger: null,
	// }
	//
	function Kafka(options) {
		this.topics = {}
		this.options = options || {}
		this.connector = null
		this.groupId = this.options.groupId || 'franz-kafka'
		this.consumerId = genConsumerId(this.groupId)
		this.onBrokerAdded = brokerAdded.bind(this)
		this.onBrokerRemoved = brokerRemoved.bind(this)
		this.allBrokers = new BrokerPool()
		this.allBrokers.once('added', this.onBrokerAdded)
		this.allBrokers.on('removed', this.onBrokerRemoved)
		this.topicDefaults = this.defaultOptions(options)
		EventEmitter.call(this)
	}
	inherits(Kafka, EventEmitter)

	function genConsumerId(groupId) {
		var rand = Buffer(4)
		rand.writeUInt32BE(Math.floor(Math.random() * 0xFFFFFFFF), 0)
		return groupId + '_' + os.hostname() + '-' + Date.now() + '-' + rand.toString('hex')
	}

	function setCompression(string) {
		var compression
		switch (string && string.toLowerCase()) {
			case 'gzip':
				compression = Compression.GZIP
				break;
			case 'snappy':
				compression = Compression.SNAPPY
				break;
			default:
				compression = Compression.NONE
				break;
		}
		return compression
	}

	Kafka.prototype.defaultOptions = function (options) {
		var defaults = {}
		options = options || {}
		defaults.queueTime = options.queueTime || 5000
		defaults.batchSize = options.batchSize || 200
		defaults.minFetchDelay = options.minFetchDelay || 0
		defaults.maxFetchDelay = options.maxFetchDelay || 10000
		defaults.maxFetchSize = options.maxFetchSize || (300 * 1024)
		defaults.maxMessageSize = options.maxMessageSize || 1000000
		defaults.compression = setCompression(options.compression)
		defaults.partitions = null
		return defaults
	}

	// Connect to your friendly neighborhood Kafka cluster.
	// onconnect will be called when the first broker is available
	//
	// onconnect: function () {}
	Kafka.prototype.connect = function (onconnect) {
		if (this.options.zookeeper) {
			this.connector = new ZKConnector(this, this.allBrokers, this.options)
		}
		else if (this.options.brokers) {
			this.connector = new StaticConnector(this, this.allBrokers, this.options)
		}
		if (typeof(onconnect) === 'function') {
			this.once('connect', onconnect)
		}
	}

	Kafka.prototype.close = function () {
		this.connector.close()
		this.allBrokers.close()
		var topicNames = Object.keys(this.topics)
		for (var i = 0; i < topicNames.length; i++) {
			var name = topicNames[i]
			this.topics[name].destroy()
			delete this.topics[name]
		}
	}

	function setTopicOptions(topicOptions, defaults) {
		topicOptions = topicOptions || {}
		var keys = Object.keys(defaults)
		var options = {}
		for (var i = 0; i < keys.length; i++) {
			var name = keys[i]
			options[name] = topicOptions[name] || defaults[name]
		}
		return options
	}

	// Create or get a topic
	//
	// name: string
	// options: {
	//   minFetchDelay: 0,      // defaults to the kafka.minFetchDelay
	//   maxFetchDelay: 10000,  // defaults to the kafka.maxFetchDelay
	//   maxFetchSize: 1000000, // defaults to the kafka.maxFetchSize
	//   compression: 'none',   // defaults to the kafka.compression
	//   batchSize: 200,        // defaults to the kafka.batchSize
	//   queueTime: 5000,       // defaults to the kafka.queueTime
	//   partitions: {
	//   	consume: ['0-0:0'],   // array of strings with the form:
	//                          //   'brokerId-partitionId:startOffset'
	//   	produce: ['0:1']      // array of strings with the form:
	//                          //   'brokerId:partitionCount'
	//   }
	// }
	// returns: a Topic object
	Kafka.prototype.topic = function (name, options) {
		var topic = this.topics[name] ||
			new Topic(
				name,
				this,
				setTopicOptions(options, this.topicDefaults)
			)
		this.topics[name] = topic
		return topic
	}

	// Register the topic with the connector.
	//
	// topic: a Topic object
	//
	// Specifically, this is for use with the ZooKeeper connector. For the static
	// connector this is a no-op
	Kafka.prototype.register = function (topic) {
		this.connector.register(topic)
	}

	// Get a broker by id from the list of all brokers.
	//
	// id: the number of the broker in it's configuration
	// returns: a Broker object or undefined
	Kafka.prototype.broker = function (id) {
		if (id === undefined) {
			return this.allBrokers.all()[0]
		}
		return this.allBrokers.get(id)
	}

	// Event handlers

	function brokerAdded(broker) {
		this.emit('connect')
	}

	function brokerRemoved(broker) {
		broker.destroy()
	}

	return Kafka
}
