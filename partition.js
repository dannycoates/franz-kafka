module.exports = function (logger) {


	function exponentialBackoff(attempt, delay) {
		return Math.floor(
			Math.random() * Math.pow(2, attempt) * 10 + delay
		)
	}

	function handleResponse(err, length, messages) {
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
		if (this.broker.isReady()) {
			this.broker.fetch(
				this.topic,
				this,
				this.fetchResponder
			)
		}
		else {
			this._setFetchDelay(true)
			this._loop()
		}
	}

	function Partition(topic, broker, id, offset) {
		this.topic = topic
		this.broker = broker
		this.id = id
		this.fetchDelay = this.topic.minFetchDelay
		this.emptyFetches = offset || 0
		this.offset = 0
		this.fetcher = fetch.bind(this)
		this.fetchResponder = handleResponse.bind(this)
		this.paused = true
		this.bufferedMessages = null
		this.timer = null
	}

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

	Partition.prototype.name = function () {
		return this.broker.id + '-' + this.id
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

	Partition.prototype.saveOffset = function (saver) {
		saver.saveOffset(this)
	}

	return Partition
}
