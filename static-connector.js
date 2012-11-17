module.exports = function (
	logger,
	inherits,
	EventEmitter,
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
	function StaticConnector(kafka, options) {
		this.kafka = kafka
		this.options = options
		this.onBrokerConnect = addBroker.bind(this)

		for (var i = 0; i < this.options.brokers.length; i++) {
			var b = this.options.brokers[i]
			var broker = new Broker(b.id, { host: b.host, port: b.port })
			broker.once('connect', this.onBrokerConnect)
			broker.connect()
		}
		EventEmitter.call(this)
	}
	inherits(StaticConnector, EventEmitter)

	function addBroker(broker) {
		this.kafka.addBroker(broker)
	}

	return StaticConnector
}
