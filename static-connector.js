module.exports = function (
	logger,
	inherits,
	EventEmitter,
	Producer,
	Consumer,
	BrokerPool,
	Broker
	) {

	// options: {
	//   brokers: [
	//     {
	//       id:
	//       host:
	//       port:
	//     }
	//   ]
	// }
	function StaticConnector(options) {
		this.options = options
		this.allBrokers = new BrokerPool('all')
		this.producer = new Producer(this.allBrokers)
		this.consumer = new Consumer(this, options.groupId, this.allBrokers)
		this.onBrokerConnect = addBroker.bind(this)
		this.onBrokerReady = brokerReady.bind(this)

		this.allBrokers.once(
			'brokerAdded',
			function (broker) {
				this.emit('brokerAdded', broker)
			}.bind(this)
		)

		for (var i = 0; i < this.options.brokers.length; i++) {
			var b = this.options.brokers[i]
			var broker = new Broker(b.id, b.host, b.port, this.options)
			broker.once('connect', this.onBrokerConnect)
			broker.on('ready', this.onBrokerReady)
		}
		EventEmitter.call(this)
	}
	inherits(StaticConnector, EventEmitter)

	function addBroker(broker) {
		this.allBrokers.add(broker)
	}

	function brokerReady(broker) {
		this.emit('brokerReady', broker)
	}

	StaticConnector.prototype.consume = function (topic, partitions) {
		logger.assert(partitions)
		this.consumer.consume(topic, partitions)
	}

	StaticConnector.prototype.stopConsuming = function (topic, partitions) {
		this.consumer.stop(topic, partitions)
	}

	StaticConnector.prototype.saveOffset = function (partition) {
		logger.info(
			'saving', partition.id,
			'broker', partition.broker.id,
			'offset', partition.offset
		)
		//TODO actually save
	}

	return StaticConnector
}
