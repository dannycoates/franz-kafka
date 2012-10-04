module.exports = function (
	inherits,
	EventEmitter) {

	function BrokerPool() {
		this.brokers = {}
		this.brokerList = []
		this.currentIndex = 0
	}
	inherits(BrokerPool, EventEmitter)

	BrokerPool.prototype.add = function (broker) {
		this.brokers[broker.id] = broker
		this.brokerList.push(broker)
		this.emit('brokerAdded', broker)
	}

	BrokerPool.prototype.remove = function (id) {
		var b = this.brokers[id]
		var i = this.brokerList.indexOf(b)
		this.brokerList.splice(i, 1)
		delete this.brokers[id]
		this.emit('brokerRemoved', b)
	}

	BrokerPool.prototype.removeBrokersNotIn = function (ids) {
		var self = this
		this.brokerList.forEach(
			function (b) {
				if (ids.indexOf(b.id) < 0) {
					self.remove(b.id)
				}
			}
		)
	}

	BrokerPool.prototype.contains = function (id) {
		return !!this.brokers[id]
	}

	BrokerPool.prototype.fetch = function () {

	}

	BrokerPool.prototype.produce = function (topic, messages) {
		this.brokerList[this.currentIndex].produce(topic, messages)
		this.currentIndex = (this.currentIndex + 1) % this.brokerList.length
	}

	return BrokerPool
}
