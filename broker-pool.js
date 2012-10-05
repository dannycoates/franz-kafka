module.exports = function (
	inherits,
	EventEmitter) {

	function TopicBrokers() {
		this.brokers = []
		this.current = 0
	}

	TopicBrokers.prototype.remove = function (broker) {
		var i = this.brokers.indexOf(broker)
		if (i >= 0) {
			this.brokers.splice(i, 1)
		}
	}

	TopicBrokers.prototype.add = function (broker) {
		if (this.brokers.indexOf(broker) < 0) {
			this.brokers.push(broker)
		}
	}

	TopicBrokers.prototype.next = function () {
		this.current = (this.current + 1) % this.brokers.length
		return this.brokers[this.current]
	}

	var nullTopicBrokers = new TopicBrokers()

	function BrokerPool() {
		this.brokers = {}
		this.topicBrokers = {}
	}
	inherits(BrokerPool, EventEmitter)

	BrokerPool.prototype.add = function (broker) {
		this.brokers[broker.id] = broker
		this.emit('brokerAdded', broker)
	}

	BrokerPool.prototype.remove = function (id) {
		var self = this
		var b = this.brokers[id]
		Object.keys(this.topicBrokers).forEach(
			function (name) {
				var tb = self.topicBrokers[name]
				tb.remove(b)
			}
		)
		delete this.brokers[id]
		this.emit('brokerRemoved', b)
	}

	BrokerPool.prototype.removeBrokersNotIn = function (ids) {
		var self = this
		Object.keys(this.brokers).forEach(
			function (id) {
				if (ids.indexOf(id) < 0) {
					self.remove(id)
				}
			}
		)
	}

	BrokerPool.prototype.setBrokerTopicPartitionCount = function (id, name, count) {
		var b = this.get(id)
		if (b) {
			var topicBrokers = this.topicBrokers[name] || new TopicBrokers()
			topicBrokers.add(b)
			this.topicBrokers[name] = topicBrokers
			b.setTopicPartitions(name, count)
		}
	}

	BrokerPool.prototype.get = function (id) {
		return this.brokers[id]
	}

	BrokerPool.prototype.contains = function (id) {
		return !!this.get(id)
	}

	BrokerPool.prototype.random = function () {
		var ids = Object.keys(this.brokers)
		return this.brokers[ids[(Math.floor(Math.random() * ids.length))]]
	}

	BrokerPool.prototype.brokerForTopic = function (name) {
		return (this.topicBrokers[name] || nullTopicBrokers).next()
	}

	BrokerPool.prototype.fetch = function () {

	}

	BrokerPool.prototype.produce = function (topic, messages) {
		var broker = this.brokerForTopic(topic.name) || this.random()
		if (broker) {
			return broker.produce(topic, messages)
		}
		return false
	}

	return BrokerPool
}
