module.exports = function (Partition) {

	function Owner(topic, brokers) {
		this.topic = topic
		this.brokers = brokers
		this.partitionsByName = {}
		this.partitions = []
		this.paused = true
	}

	Owner.prototype.consume = function (partitionNamesWithOffsets) {
		this.paused = false
		for (var i = 0; i < partitionNamesWithOffsets.length; i++) {
			var nameAndOffset = partitionNamesWithOffsets[i].split(':')
			var name = nameAndOffset[0]
			var offset = +(nameAndOffset[1] || 0)
			var brokerPartition = name.split('-')
			if (brokerPartition.length === 2) {
				var brokerId = +brokerPartition[0]
				var partitionNo = +brokerPartition[1]
				var broker = this.brokers.get(brokerId)
				var partition = this.partitionsByName[name] ||
					new Partition(this.topic, broker, partitionNo, offset)

				this.partitionsByName[name] = partition
				if(this.partitions.indexOf(partition) === -1) {
					this.partitions.push(partition)
				}
				partition.reset()
			}
		}
	}

	Owner.prototype.stop = function (partitionNames) {
		if (!partitionNames) { // stop all
			partitionNames = Object.keys(this.partitionsByName)
		}
		for (var i = 0; i < partitionNames.length; i++) {
			var name = partitionNames[i]
			var p = this.partitionsByName[name]
			if (p) {
				p.pause()
				var x = this.partitions.indexOf(p)
				if (x >= 0) {
					this.partitions.splice(x, 1)
				}
				delete this.partitionsByName[name]
			}
		}
	}

	Owner.prototype.hasPartitions = function () {
		return this.partitions.length > 0
	}

	function pausePartition(p) { p.pause() }

	Owner.prototype.pause = function () {
		if (!this.paused) {
			this.partitions.forEach(pausePartition)
		}
		this.paused = true
	}

	function resumePartition(p) { p.resume() }

	Owner.prototype.resume = function () {
		if (this.paused) {
			this.partitions.forEach(resumePartition)
		}
		this.paused = false
	}

	Owner.prototype.saveOffsets = function (saver) {
		this.partitions.forEach(
			function (p) {
				p.saveOffset(saver)
			}
		)
	}

	return Owner
}
