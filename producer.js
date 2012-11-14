module.exports = function (
	inherits,
	EventEmitter,
	BrokerPool) {

	function Producer(allBrokers) {
		this.allBrokers = allBrokers
		this.topicBrokers = {}
		EventEmitter.call(this)
	}
	inherits(Producer, EventEmitter)

	Producer.prototype._remove = function (broker) {
		var names = Object.keys(this.topicBrokers)
		for (var i = 0; i < names.length; i++) {
			var name = names[i]
			this.topicBrokers[name].remove(broker)
		}
	}

	Producer.prototype.removeBrokersNotIn = function (ids) {
		var brokers = this.allBrokers.all()
		for (var i = 0; i < brokers.length; i++) {
			var b = brokers[i]
			if (ids.indexOf(b.id) < 0) {
				this._remove(b)
			}
		}
	}

	Producer.prototype.addPartitions = function (topicName, partitionNames) {
		if (!Array.isArray(partitionNames)) {
			return
		}
		for (var i = 0; i < partitionNames.length; i++) {
			var name = partitionNames[i]
			var split = name.split(':')
			if (split.length === 2) {
				var brokerId = +split[0]
				var partitionCount = +split[1]
				this.setBrokerTopicPartitionCount(brokerId, topicName, partitionCount)
			}
		}
	}

	Producer.prototype.setBrokerTopicPartitionCount = function (id, name, count) {
		var b = this.allBrokers.get(id)
		if (b) {
			var topicBrokers = this.topicBrokers[name] || new BrokerPool(name)
			topicBrokers.add(b)
			this.topicBrokers[name] = topicBrokers
			b.setTopicPartitions(name, count)
		}
	}

	Producer.prototype.brokerForTopic = function (name) {
		return (this.topicBrokers[name] || BrokerPool.nil).nextReady()
	}

	Producer.prototype.write = function (topic, messages, cb) {
		var broker = this.brokerForTopic(topic.name)
		if (broker) {
			var ready = broker.write(topic, messages, cb) || this.isReady(topic)
			topic.setReady(ready)
			return ready
		}
		// new topic
		// XXX im not sure how to best handle this case.
		// for instance if you blast a bunch of writes
		// before the broker-partition assignments arrive
		this.allBrokers.randomReady().write(topic, messages, cb)
		return true
	}

	Producer.prototype.isReady = function (topic) {
		return this.topicBrokers[topic.name].areAnyReady()
	}

	return Producer
}
