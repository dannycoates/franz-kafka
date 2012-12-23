module.exports = function (logger, inherits, EventEmitter) {

	// A collection of Broker objects accessible by id or all
	function BrokerPool() {
		this.brokers = []
		this.brokersById = {}
		EventEmitter.call(this)
	}
	inherits(BrokerPool, EventEmitter)

	BrokerPool.prototype.remove = function (broker) {
		var i = this.brokers.indexOf(broker)
		if (i >= 0) {
			this.brokers.splice(i, 1)
			delete this.brokersById[broker.id]
			logger.info('removed', broker.id)
			this.emit('removed', broker)
		}
	}

	BrokerPool.prototype.add = function (broker) {
		if (this.brokers.indexOf(broker) < 0) {
			this.brokers.push(broker)
			this.brokersById[broker.id] = broker
			logger.info('added', broker.id)
			this.emit('added', broker)
		}
	}

	BrokerPool.prototype.get = function (id) {
		return this.brokersById[id]
	}

	BrokerPool.prototype.all = function () {
		return this.brokers
	}

	BrokerPool.prototype.close = function () {
		while (this.brokers.length) {
			this.remove(this.brokers[this.brokers.length - 1])
		}
	}

	return BrokerPool
}
