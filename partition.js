module.exports = function (logger) {
	function handleResponse(err, length, messages) {
		if (err) {
			return this.topic.error(err)
		}
		if (length === 0) {
			//TODO no new messages, backoff
			return
		}
		this.offset += length
		if (this.paused) {
			this.bufferedMessages = messages
		}
		else {
			this.topic.parseMessages(messages)
			this._loop()
		}
	}

	function fetch() {
		this.broker.fetch(
			this.topic,
			this.partition,
			this.maxSize,
			this.fetchResponder
		)
	}

	function Partition(topic, broker, partition) {
		this.topic = topic
		this.broker = broker
		this.partition = partition
		this.interval = this.topic.interval
		this.offset = 0
		this.maxSize = 300 * 1024 //TODO set via option
		this.fetcher = fetch.bind(this)
		this.fetchResponder = handleResponse.bind(this)
		this.paused = false
		this.bufferedMessages = null
		this.timer = null
	}

	Partition.prototype._loop = function () {
		if (this.interval) {
			this.timer = setTimeout(this.fetcher, this.interval)
		}
	}

	Partition.prototype.flush = function () {
		if (this.bufferedMessages) {
			this.topic.parseMessages(this.bufferedMessages)
			this.bufferedMessages = null
		}
	}

	Partition.prototype.resume = function () {
		logger.info(
			'resume', this.topic.name,
			'broker', this.broker.id,
			'partition', this.partition
		)
		this.paused = false
		this.flush()
		this.fetcher()
	}

	Partition.prototype.pause = function () {
		logger.info(
			'pause', this.topic.name,
			'broker', this.broker.id,
			'partition', this.partition
		)
		this.paused = true
		clearTimeout(this.timer)
	}

	Partition.prototype.reset = function () {
		this.pause()
		this.resume()
	}

	return Partition
}
