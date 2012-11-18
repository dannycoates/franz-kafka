module.exports = function (
	logger,
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
		this.onPartitionDestroy = partitionDestroy.bind(this)
		this.readables = {}
		this.writables = {}
		EventEmitter.call(this)
	}
	inherits(PartitionSet, EventEmitter)

	PartitionSet.prototype.get = function (name) {
		return this.partitionsByName[name]
	}

	PartitionSet.prototype.add = function (partition) {
		if (this.partitions.indexOf(partition) < 0) {
			partition.on('writable', this.onWritableChanged)
			partition.on('readable', this.onReadableChanged)
			partition.on('ready', this.onPartitionReady)
			partition.on('destroy', this.onPartitionDestroy)
			this.partitionsByName[partition.name] = partition
			this.partitions.push(partition)
			logger.info('added partition', partition.name)
		}
	}

	PartitionSet.prototype.remove = function (partition) {
		var i = this.partitions.indexOf(partition)
		if (i >= 0) {
			var p = this.partitions[i]
			var name = p.name
			p.removeListener('writable', this.onWritableChanged)
			p.removeListener('readable', this.onReadableChanged)
			p.removeListener('ready', this.onPartitionReady)
			p.removeListener('destroy', this.onPartitionDestroy)
			delete this.partitionsByName[name]
			delete this.readables[name]
			delete this.writables[name]
			this.partitions.splice(i, 1)
			logger.info('removed partition', name)
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

	function readableChanged(partition) {
		if (partition.isReadable()) {
			this.readables[partition.name] = partition
		}
		else {
			delete this.readables[partition.name]
		}
	}

	function writableChanged(partition) {
		if (partition.isWritable()) {
			this.writables[partition.name] = partition
			if (this.isReady()) {
				this.emit('ready')
			}
		}
		else {
			delete this.writables[partition.name]
		}
	}

	function partitionReady(partition) {
		this.emit('ready')
	}

	function partitionDestroy(partition) {
		this.remove(partition)
	}

	return PartitionSet
}
