module.exports = function (logger, inherits, EventEmitter, Broker) {

	// A Partition represents the location of messages for a Topic.
	// Partitions may be 'readable' and/or 'writable'.
	function Partition(topic, broker, id) {
		this.topic = topic
		this.broker = broker
		this.id = id
		this.name = this.broker.id + '-' + this.id
		this.offset = 0
		this.readable = null
		this.writable = null
		this.onBrokerReady = brokerReady.bind(this)
		this.onBrokerDestroy = brokerDestroy.bind(this)
		this.broker.on('ready', this.onBrokerReady)
		this.broker.on('destroy', this.onBrokerDestroy)
		EventEmitter.call(this)
	}
	inherits(Partition, EventEmitter)

	Partition.prototype.write = function (messages, cb) {
		return this.broker.write(this, messages, cb)
	}

	Partition.prototype.isReady = function () {
		return this.broker.isReady()
	}

	Partition.prototype.isWritable = function (writable) {
		if (writable !== undefined && this.writable !== writable) {
			this.writable = writable
			this.emit('writable', this)
		}
		return this.writable
	}

	Partition.prototype.isReadable = function (readable) {
		if (readable !== undefined && this.readable !== readable) {
			this.readable = readable
			this.emit('readable', this)
		}
		return this.readable
	}

	Partition.prototype.fetch = function(cb) {
		this.broker.fetch(this.topic, this, fetchResponse.bind(this, cb))
	}

	Partition.prototype.minFetchDelay = function () {
		return this.topic.minFetchDelay
	}

	Partition.prototype.maxFetchDelay = function () {
		return this.topic.maxFetchDelay
	}

	Partition.prototype.toString = function () {
		return '(topic ' + this.topic.name + ' partition ' + this.name + ')'
	}

	function fetchResponse(cb, err, length, messages) {
		if (!err) {
			this.offset += length
		}
		cb(err, messages)
	}

	function brokerReady() {
		this.emit('ready', this)
	}

	function brokerDestroy() {
		this.isWritable(false)
		this.isReadable(false)
		this.broker.removeListener('ready', this.onBrokerReady)
		this.broker.removeListener('destroy', this.onBrokerDestroy)
		this.broker = Broker.nil
		this.emit('destroy', this)
	}

	return Partition
}
