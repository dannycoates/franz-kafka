module.exports = function () {

	function Owner(brokers) {
		this.brokers = brokers
		this.interval = 0
	}

	Owner.prototype.addPartitions = function (partitions) {

	}

	Owner.prototype.consume = function () {

	}

	return Owner
}