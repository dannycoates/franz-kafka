module.exports = function (
	os,
	inherits,
	EventEmitter,
	Owner) {

	function genConsumerId(groupId) {
		return groupId + '_' + os.hostname() + '-' + Date.now() + '-' + "DEADBEEF"
	}

	function Consumer(groupId, allBrokers) {
		this.groupId = groupId
		this.consumerId = genConsumerId(this.groupId)
		this.allBrokers = allBrokers
		this.owners = {}
	}

	Consumer.prototype.consume = function (topic, interval, partitions) {
		console.assert(Array.isArray(partitions))
		var name = topic.name
		var owner = this.owners[name] || new Owner(topic, this.allBrokers)
		this.owners[name] = owner
		owner.consume(partitions, interval)
	}

	Consumer.prototype.stop = function (topic, partitions) {
		if (!topic) { // stop all
			var topics = Object.keys(this.owners)
			for (var i = 0; i < topics.length; i++) {
				this.stop(topics[i])
			}
		}
		var name = topic.name
		var owner = this.owners[name]
		owner.stop(partitions)
		if (!owner.hasPartitions()) {
			delete this.owners[name]
		}
	}

	return Consumer
}
