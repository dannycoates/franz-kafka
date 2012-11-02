module.exports = function () {

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
		this.broker.fetch(this.topic, this.partition, this.maxSize, this.fetchResponder)
	}

	function Partition(topic, broker, partition) {
		this.topic = topic
		this.broker = broker
		this.partition = partition
		this.interval = 0
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

	function Owner(topic, brokers) {
		this.topic = topic
		this.brokers = brokers
		this.partitions = {}
	}

	Owner.prototype.consume = function (partitions, interval) {
		for (var i = 0; i < partitions.length; i++) {
			var name = partitions[i]
			var split = name.split('-')
			if (split.length === 2) {
				var brokerId = +split[0]
				var partition = +split[1]
				var broker = this.brokers.get(brokerId)
				var pc = this.partitions[name] || new Partition(this.topic, broker, partition)
				pc.interval = interval
				pc.reset()
				this.partitions[name] = pc
			}
		}
	}

	Owner.prototype.stop = function (partitions) {
		if (!partitions) { // stop all
			partitions = Object.keys(this.partitions)
		}
		for (var i = 0; i < partitions.length; i++) {
			var name = partitions[i]
			var p = this.partitions[name]
			if (p) {
				p.stop()
				delete this.partitions[name]
			}
		}
	}

	Owner.prototype.hasPartitions = function () {
		return Object.keys(this.partitions).length > 0
	}

	return Owner
}
