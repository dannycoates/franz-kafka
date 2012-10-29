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

	TopicBrokers.prototype.nextReady = function () {
		for (var i = 0; i < this.brokers.length; i++) {
			var b = this.next()
			if (b.ready()) {
				break
			}
		}
		return b
	}

	TopicBrokers.prototype.someReady = function () {
		return this.brokers.some(function (b) { return b.ready() })
	}

	var nullTopicBrokers = new TopicBrokers()

	function BrokerPool() {
		var self = this
		this.brokers = {}
		this.topicBrokers = {}
		this.brokerReady = function () {
			self.emit('brokerReady', this)
		}
	}
	inherits(BrokerPool, EventEmitter)

	BrokerPool.prototype.add = function (broker) {
		this.brokers[broker.id] = broker
		broker.on('ready', this.brokerReady)
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
		b.removeListener('ready', this.brokerReady)
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

	BrokerPool.prototype.randomReady = function () {
		var ids = Object.keys(this.brokers)
		var n = (Math.floor(Math.random() * ids.length))
		for (var i = 0; i < ids.length; i++) {
			var b = this.brokers[ids[n]]
			if (b.ready()) {
				break
			}
			n = (n + 1) % ids.length
		}
		return b
	}

	BrokerPool.prototype.brokerForTopic = function (name) {
		return (this.topicBrokers[name] || nullTopicBrokers).nextReady()
	}

	BrokerPool.prototype.fetch = function () {

	}

	BrokerPool.prototype.publish = function (topic, messages) {
		var broker = this.brokerForTopic(topic.name)
		if (broker) {
			var ready = broker.publish(topic, messages) ||
				this.topicBrokers[topic.name].someReady()
			topic.setReady(ready)
			return ready
		}
		// new topic
		// XXX im not sure how to best handle this case.
		// for instance if you blast a bunch of publishes
		// before the broker-partition assignments arrive
		this.randomReady().publish(topic, messages)
		return true
	}

	return BrokerPool
}
