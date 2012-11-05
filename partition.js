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
		this.topic.parseMessages(messages)
		this._loop()
	}

	function fetch() {
		logger.log('fetching', this.topic.name, 'broker', this.broker.id, 'partition', this.partition)
		this.broker.fetch(this.topic, this.partition, this.maxSize, this.fetchResponder)
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
		this.timer = null
	}

	Partition.prototype._loop = function () {
		if (this.interval) {
			this.timer = setTimeout(this.fetcher, this.interval)
		}
	}

	Partition.prototype.start = function () {
		this.fetcher()
	}

	Partition.prototype.stop = function () {
		clearTimeout(this.timer)
	}

	Partition.prototype.reset = function () {
		this.stop()
		this.start()
	}

	Partition.prototype.drain = function (cb) {
		this.stop()
		this.broker.drain(cb)
		//TODO when to start again?
	}

	return Partition
}
