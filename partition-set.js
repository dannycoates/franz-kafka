module.exports = function (
	inherits,
	EventEmitter,
	Partition) {

	function PartitionSet() {
		this.partitionsByName = {}
		this.partitions = []
		this.current = 0
		this.onReadableChanged = readableChanged.bind(this)
		this.onWritableChanged = writableChanged.bind(this)
		this.onPartitionReady = partitionReady.bind(this)
		this.readables = {}
		this.writables = {}
		EventEmitter.call(this)
	}
	inherits(PartitionSet, EventEmitter)

	function readableChanged(partition) {
		if (partition.isReadable()) {
			this.readables[partition.name()] = partition
		}
		else {
			delete this.readables[partition.name()]
		}
	}

	function writableChanged(partition) {
		if (partition.isWritable()) {
			this.writables[partition.name()] = partition
		}
		else {
			delete this.writables[partition.name()]
		}
	}

	function partitionReady(partition) {
		this.emit('ready')
	}

	PartitionSet.prototype.get = function (name) {
		return this.partitionsByName[name]
	}

	PartitionSet.prototype.add = function (partition) {
		if (this.partitions.indexOf(partition) < 0) {
			partition.on('writable', this.onWritableChanged)
			partition.on('readable', this.onReadableChanged)
			partition.on('ready', this.onPartitionReady)
			this.partitionsByName[partition.name()] = partition
			this.partitions.push(partition)
		}
	}

	PartitionSet.prototype.remove = function (partition) {
		var i = this.partitions.indexOf(partition)
		if (i >= 0) {
			var p = this.partitions[i]
			p.removeListener('writable', this.onWritableChanged)
			p.removeListener('readable', this.onReadableChanged)
			p.removeListener('ready', this.onPartitionReady)
			delete this.partitionsByName[p.name()]
			this.partitions.splice(i, 1)
		}
	}

	PartitionSet.prototype.next = function () {
		this.current = (this.current + 1) % this.partitions.length
		return this.partitions[this.current]
	}

	PartitionSet.prototype.all = function () {
		return this.partitions
	}

	PartitionSet.prototype.isReady = function () {
		return this.partitions.some(
			function (p) {
				return p.isReady() && p.isWritable()
			}
		)
	}

	PartitionSet.prototype.nextWritable = function () {
		var partition = Partition.nil
		for (var i = 0; i < this.partitions.length; i++) {
			partition = this.next()
			if (partition.isWritable() && partition.isReady()) {
				break;
			}
		}
		return partition
	}

	PartitionSet.prototype.length = function () {
		return this.partitions.length
	}

	function readablePartition(p) { return p.isReadable() }

	PartitionSet.prototype.readable = function () {
		return this.partitions.filter(readablePartition)
	}

	function pausePartition(p) { p.pause() }

	PartitionSet.prototype.pause = function () {
		this.readable().forEach(pausePartition)
	}

	function resumePartition(p) { p.resume() }

	PartitionSet.prototype.resume = function () {
		this.readable().forEach(resumePartition)
	}

	function stopPartition(p) { p.stop() }

	PartitionSet.prototype.stop = function () {
		this.readable().forEach(stopPartition)
	}

	PartitionSet.nil = new PartitionSet()

	return PartitionSet
}
