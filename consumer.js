module.exports = function (os, inherits, EventEmitter) {

	function genConsumerId(groupId) {
		return groupId + '_' + os.hostname() + '-' + Date.now() + '-' + "DEADBEEF"
	}

	function Consumer(groupId, allBrokers) {
		this.groupId = groupId
		this.consumerId = genConsumerId(this.groupId)
		this.allBrokers = allBrokers
		this.topics = {}
	}

	Consumer.prototype.consume = function (topic) {

	}

	Consumer.prototype.fetch = function (topic) {

	}

	return Consumer
}
