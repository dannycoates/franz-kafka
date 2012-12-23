module.exports = function (logger, inherits, EventEmitter, Broker) {

	// options: {
	//   brokers: [
	//     {
	//       id:
	//       host:
	//       port:
	//     }
	//   ]
	// }
	function StaticConnector(kafka, brokers, options) {
		this.brokers = brokers
		this.onBrokerConnect = brokerConnect.bind(this)

		for (var i = 0; i < options.brokers.length; i++) {
			var b = options.brokers[i]
			var broker = new Broker(b.id, { host: b.host, port: b.port })
			broker.once('connect', this.onBrokerConnect)
			broker.connect()
		}
		EventEmitter.call(this)
	}
	inherits(StaticConnector, EventEmitter)

	function brokerConnect(broker) {
		this.brokers.add(broker)
	}

	StaticConnector.prototype.register = function (topic) {
		// noop
	}

	StaticConnector.prototype.close = function () {

	}

	return StaticConnector
}
