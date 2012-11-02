module.exports = function (
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
	//       topics : {
	//         foo: 5,
	//         bar: 2
	//       }
	//     }
	//   ]
	// }
	function StaticConnector(options) {
		var self = this
		this.options = options
		this.allBrokers = new BrokerPool()
		this.producer = new Producer(this.allBrokers)
		this.consumer = new Consumer(options.groupId, this.allBrokers)

		this.allBrokers.once('brokerAdded',
			function (broker) {
				self.emit('brokerAdded', broker)
			}
		)

		this.options.brokers.forEach(
			function (b) {
				var broker = new Broker(b.id, b.host, b.port)
				Object.keys(b.topics).forEach( //TODO would be great to get rid of this
					function (t) {
						broker.setTopicPartitions(t, b.topics[t])
					}
				)
				broker.once(
					'connect',
					function () {
						self.allBrokers.add(broker)
					}
				)
				broker.on('ready',
					function () {
						self.emit('brokerReady', this)
					}
				)
			}
		)
		EventEmitter.call(this)
	}
	inherits(StaticConnector, EventEmitter)

	StaticConnector.prototype.consume = function (topic, interval, partitions) {
		console.assert(partitions)
		this.consumer.consume(topic, interval, partitions)
	}

	StaticConnector.prototype.stopConsuming = function (topic, partitions) {
		this.consumer.stop(topic, partitions)
	}

	StaticConnector.prototype.publish = function (topic, messages) {
		return this.producer.publish(topic, messages)
	}

	return StaticConnector
}
