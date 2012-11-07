module.exports = function (Partition) {

	function Owner(topic, brokers) {
		this.topic = topic
		this.brokers = brokers
		this.partitions = {}
	}

	Owner.prototype.consume = function (partitions) {
		for (var i = 0; i < partitions.length; i++) {
			var name = partitions[i]
			var split = name.split('-')
			if (split.length === 2) {
				var brokerId = +split[0]
				var partitionNo = +split[1]
				var broker = this.brokers.get(brokerId)
				var partition = this.partitions[name] ||
					new Partition(this.topic, broker, partitionNo)
				partition.reset()
				this.partitions[name] = partition
			}
		}
	}

	Owner.prototype.stop = function (partitions) {
		if (!partitions) { // stop all
			partitions = Object.keys(this.partitions)
		}
		for (var i = 0; i < partitions.length; i++) {
			var name = partitions[i]
			var p = this.partitions[name]
			if (p) {
				p.pause()
				delete this.partitions[name]
			}
		}
	}

	Owner.prototype.hasPartitions = function () {
		return Object.keys(this.partitions).length > 0
	}

	Owner.prototype.pause = function () {
		var partitions = Object.keys(this.partitions)
		for (var i = 0; i < partitions.length; i++) {
			this.partitions[partitions[i]].pause()
		}
	}

	Owner.prototype.resume = function () {
		var partitions = Object.keys(this.partitions)
		for (var i = 0; i < partitions.length; i++) {
			this.partitions[partitions[i]].resume()
		}
	}

	return Owner
}
