module.exports = function (
	inherits,
	EventEmitter) {

	function MessageBuffer(partitions, batchSize, queueTime) {
		this.partitions = partitions
		this.batchSize = batchSize
		this.queueTime = queueTime
		this.messages = []
		this.timer = null
		this.send = send.bind(this)
		this.onProduceResponse = produceResponse.bind(this)
		EventEmitter.call(this)
	}
	inherits(MessageBuffer, EventEmitter)

	MessageBuffer.prototype.clearTimer = function () {
		clearTimeout(this.timer)
		this.timer = null
	}

	MessageBuffer.prototype.push = function (message) {
		this.messages.push(message)
		return this.flush()
	}

	MessageBuffer.prototype.flush = function () {
		if (this.messages.length >= this.batchSize) {
			return this.send()
		}
		if (!this.timer) {
			this.timer = setTimeout(this.send, this.queueTime)
		}
		return true
	}

	function produceResponse(err) {
		if (err) {
			this.emit('error', err)
		}
	}

	function batchify(messages, size) {
		if(messages.length <= size) {
			return [messages]
		}
		return messages.reduce(
			function (group, message, index) {
				var x = Math.floor(index / size)
				var bucket = group[x] || []
				bucket.push(message)
				group[x] = bucket
				return group
			},
			[]
		)
	}

	function send() {
		var sent = false
		if (this.partitions.isReady() && this.messages.length > 0) {
			var batches = batchify(this.messages, this.batchSize)
			for (var i = 0; i < batches.length; i++) {
				var partition = this.partitions.nextWritable()
				sent = partition.write(batches[i], this.onProduceResponse)
			}
			this.messages = []
		}
		this.clearTimer()
		return sent
	}

	return MessageBuffer
}
