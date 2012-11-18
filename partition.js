module.exports = function (logger, inherits, EventEmitter, Broker) {

	function Partition(topic, broker, id) {
		this.topic = topic
		this.broker = broker
		this.id = id
		this.name = this.broker.id + '-' + this.id
		this.fetchDelay = this.topic.minFetchDelay
		this.emptyFetches = 0
		this.offset = 0
		this.paused = true
		this.bufferedMessages = null
		this.timer = null
		this.readable = null
		this.writable = null
		this.fetcher = fetch.bind(this)
		this.onFetchResponse = fetchResponse.bind(this)
		this.onBrokerReady = brokerReady.bind(this)
		this.onBrokerDestroy = brokerDestroy.bind(this)
		this.broker.on('ready', this.onBrokerReady)
		this.broker.on('destroy', this.onBrokerDestroy)
		EventEmitter.call(this)
	}
	inherits(Partition, EventEmitter)

	Partition.prototype._setFetchDelay = function (shouldDelay) {
		this.emptyFetches = shouldDelay ? this.emptyFetches + 1 : 0
		this.fetchDelay = Math.min(
			exponentialBackoff(this.emptyFetches, this.topic.minFetchDelay),
			this.topic.maxFetchDelay
		)
		logger.info(
			'fetch', this.topic.name,
			'broker', this.broker.id,
			'partition', this.id,
			'delay', this.fetchDelay,
			'empty', this.emptyFetches
		)
	}

	Partition.prototype._loop = function () {
		if (this.fetchDelay) {
			this.timer = setTimeout(this.fetcher, this.fetchDelay)
		}
		else {
			this.fetcher()
		}
	}

	Partition.prototype.flush = function () {
		if (this.bufferedMessages) {
			this.topic.parseMessages(this, this.bufferedMessages)
			this.bufferedMessages = null
		}
	}

	Partition.prototype.resume = function () {
		logger.info(
			'resume', this.topic.name,
			'broker', this.broker.id,
			'partition', this.id
		)
		this.paused = false
		this.flush()
		this.fetcher()
	}

	Partition.prototype.pause = function () {
		logger.info(
			'pause', this.topic.name,
			'broker', this.broker.id,
			'partition', this.id
		)
		this.paused = true
		clearTimeout(this.timer)
	}

	Partition.prototype.reset = function () {
		this.pause()
		this.resume()
	}

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

	Partition.nil = new Partition({ minFetchDelay: 0 }, Broker.nil, -1)

	function exponentialBackoff(attempt, delay) {
		return Math.floor(
			Math.random() * Math.pow(2, attempt) * 10 + delay
		)
	}

	function fetchResponse(err, length, messages) {
		this.pending = false
		if (err) {
			return this.topic.error(err)
		}
		this.offset += length
		if (this.paused) {
			logger.info(
				'buffered', messages.length,
				'topic', this.topic.name,
				'broker', this.broker.id,
				'partition', this.id
			)
			this.bufferedMessages = messages
		}
		else {
			this.topic.parseMessages(this, messages)
			this._setFetchDelay(length === 0)
			this._loop()
		}
	}

	function fetch() {
		if (this.isReady()) {
			this.broker.fetch(
				this.topic,
				this,
				this.onFetchResponse
			)
		}
		else {
			this._setFetchDelay(true)
			this._loop()
		}
	}

	function brokerReady() {
		this.emit('ready', this)
	}

	function brokerDestroy() {
		this.pause()
		this.isWritable(false)
		this.isReadable(false)
		this.broker.removeListener('ready', this.onBrokerReady)
		this.broker.removeListener('destroy', this.onBrokerDestroy)
		this.broker = Broker.nil
		this.emit('destroy', this)
	}

	return Partition
}
