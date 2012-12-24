module.exports = function (logger, inherits, EventEmitter, RingList) {

	function Consumer() {
		EventEmitter.call(this)
		this.partitions = new RingList()
		this.pending = false
		this.paused = true
		this.emptyFetches = 0
		this.fetch = fetch.bind(this)
		this.fetchCallback = fetchCallback.bind(this)
		this.on('response', response)
		this.bufferedMessages = null
		this.timer = null
	}
	inherits(Consumer, EventEmitter)

	function exponentialBackoff(attempt, delay) {
		return Math.floor(Math.random() * Math.pow(2, attempt) * 10 + delay)
	}

	Consumer.prototype._nextFetchDelay = function (shouldDelay) {
		this.emptyFetches = shouldDelay ? this.emptyFetches + 1 : 0
		var min = 0
		var max = 10000
		var partition = this.partitions.current()
		if (partition) {
			min = partition.minFetchDelay()
			max = partition.maxFetchDelay()
		}
		return Math.min(exponentialBackoff(this.emptyFetches, min), max)
	}

	Consumer.prototype._loop = function (shouldDelay) {
		var fetchDelay = this._nextFetchDelay(shouldDelay)
		if (fetchDelay) {
			this.timer = setTimeout(this.fetch, fetchDelay)
		}
		else {
			this.fetch()
		}
	}

	Consumer.prototype.flush = function () {
		if (this.bufferedMessages) {
			this.emit('messages', this, this.bufferedMessages)
			this.bufferedMessages = null
		}
	}

	Consumer.prototype.add = function (partition) {
		this.partitions.add(partition)
	}

	Consumer.prototype.remove = function (partition) {
		this.partitions.remove(partition)
	}

	Consumer.prototype.pause = function () {
		this.paused = true
		clearTimeout(this.timer)
	}

	Consumer.prototype.resume = function () {
		this.paused = false
		this.flush()
		if (!this.paused && !this.pending) {
			this.fetch()
		}
	}

	Consumer.prototype.drain = function (cb) {
		this.pause()
		if (this.pending) {
			this.once(
				'response',
				function () {
					this.flush()
					cb()
				}.bind(this)
			)
		}
		else {
			this.flush()
			cb()
		}
	}

	function isReady(partition) {
		return partition.isReady()
	}

	function fetch() {
		clearTimeout(this.timer)
		var partition = this.partitions.next(isReady)
		if (partition) {
			this.pending = true
			partition.fetch(this.fetchCallback)
		}
		else {
			this._loop(true)
		}
	}

	function fetchCallback(err, messages) {
		this.pending = false
		this.emit('response', err, messages)
	}

	function response(err, messages) {
		if (err) {
			return this.emit('error', err)
		}
		if (this.paused) {
			this.bufferedMessages = messages
		}
		else {
			this._loop(messages.length === 0)
			this.emit('messages', this, messages)
		}
	}

	return Consumer
}
