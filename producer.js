module.exports = function (
	inherits,
	EventEmitter,
	BrokerPool) {

	function Producer(allBrokers) {
		var self = this
		this.allBrokers = allBrokers
		this.topicBrokers = {}
		EventEmitter.call(this)
	}
	inherits(Producer, EventEmitter)

	Producer.prototype._remove = function (broker) {
		var self = this
		Object.keys(this.topicBrokers).forEach(
			function (name) {
				self.topicBrokers[name].remove(broker)
			}
		)
	}

	Producer.prototype.removeBrokersNotIn = function (ids) {
		var self = this
		this.allBrokers.all().forEach(
			function (b) {
				if (ids.indexOf(b.id) < 0) {
					self._remove(b)
				}
			}
		)
	}

	Producer.prototype.setBrokerTopicPartitionCount = function (id, name, count) {
		var b = this.allBrokers.get(id)
		if (b) {
			var topicBrokers = this.topicBrokers[name] || new BrokerPool()
			topicBrokers.add(b)
			this.topicBrokers[name] = topicBrokers
			b.setTopicPartitions(name, count)
		}
	}

	Producer.prototype.brokerForTopic = function (name) {
		return (this.topicBrokers[name] || BrokerPool.nil).nextReady()
	}

	Producer.prototype.publish = function (topic, messages) {
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
		this.allBrokers.randomReady().publish(topic, messages)
		return true
	}

	return Producer
}
