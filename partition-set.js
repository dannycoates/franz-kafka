module.exports = function (logger, inherits, EventEmitter, Consumer, Producer) {

	// A PartitionSet contains all of the known Partitions (for a Topic)
	// It tracks which partitions are 'readable' and 'writable'
	function PartitionSet() {
		this.partitions = {}
		this.consumer = new Consumer()
		this.producer = new Producer()

		this.onConsumerMessages = consumerMessages.bind(this)
		this.onConsumerError = consumerError.bind(this)
		this.onReadableChanged = readableChanged.bind(this)
		this.onWritableChanged = writableChanged.bind(this)
		this.onPartitionReady = partitionReady.bind(this)
		this.onPartitionDestroy = partitionDestroy.bind(this)

		this.consumer.on('messages', this.onConsumerMessages)
		this.consumer.on('error', this.onConsumerError)
		EventEmitter.call(this)
	}
	inherits(PartitionSet, EventEmitter)

	PartitionSet.prototype.get = function (name) {
		return this.partitions[name]
	}

	PartitionSet.prototype.add = function (partition) {
		if(!this.partitions[partition.name]) {
			partition.on('writable', this.onWritableChanged)
			partition.on('readable', this.onReadableChanged)
			partition.on('ready', this.onPartitionReady)
			partition.on('destroy', this.onPartitionDestroy)
			this.partitions[partition.name] = partition
			logger.info('added partition', partition.name)
		}
	}

	PartitionSet.prototype.remove = function (partition) {
		this.consumer.remove(partition)
		this.producer.remove(partition)
		partition.removeListener('writable', this.onWritableChanged)
		partition.removeListener('readable', this.onReadableChanged)
		partition.removeListener('ready', this.onPartitionReady)
		partition.removeListener('destroy', this.onPartitionDestroy)
		delete this.partitions[partition.name]
		logger.info('removed partition', partition.name)
	}

	PartitionSet.prototype.isReady = function () {
		return this.producer.isReady()
	}

	PartitionSet.prototype.pause = function () {
		this.consumer.pause()
	}

	PartitionSet.prototype.resume = function () {
		this.consumer.resume()
	}

	PartitionSet.prototype.stopConsuming = function () {
		var names = Object.keys(this.partitions)
		for (var i = 0; i < names.length; i++) {
			var partition = this.partitions[names[i]]
			partition.isReadable(false)
		}
	}

	PartitionSet.prototype.drain = function (cb) {
		this.consumer.drain(cb)
	}

	PartitionSet.prototype.write = function (messages, cb) {
		this.producer.write(messages, cb)
	}

	// Event handlers

	function consumerMessages(partition, messages) {
		this.emit('messages', partition, messages)
	}

	function consumerError(err) {

	}

	function readableChanged(partition) {
		if (partition.isReadable()) {
			this.consumer.add(partition)
		}
		else {
			this.consumer.remove(partition)
		}
	}

	function writableChanged(partition) {
		if (partition.isWritable()) {
			this.producer.add(partition)
			// TODO: emit less
			if (this.isReady()) {
				this.emit('ready')
			}
		}
		else {
			this.producer.remove(partition)
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
