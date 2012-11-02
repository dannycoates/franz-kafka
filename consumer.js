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
		var owner = this.owners[topic.name] || new Owner(this.allBrokers)
		owner.addPartitions(partitions)
		owner.interval = interval
		this.owners[topic.name] = owner
		owner.consume()
	}

	return Consumer
}
