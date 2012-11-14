module.exports = function () {

	function handleResponse(err) {
		if (err) {
			this.topic.error(err)
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
		if (this.producer.isReady(this.topic)) {
			var batches = batchify(this.messages, this.batchSize)
			for (var i = 0; i < batches.length; i++) {
				sent = this.producer.write(
					this.topic,
					batches[i],
					this.produceResponder
				)
			}
			this.reset()
		}
		return sent
	}

	function MessageBuffer(topic, batchSize, queueTime, producer) {
		var self = this
		this.topic = topic
		this.batchSize = batchSize
		this.queueTime = queueTime
		this.producer = producer
		this.messages = []
		this.timer = null
		this.send = send.bind(this)
		this.produceResponder = handleResponse.bind(this)
	}

	MessageBuffer.prototype.reset = function () {
		this.messages = []
		clearTimeout(this.timer)
		this.timer = null
	}

	MessageBuffer.prototype.push = function(message) {
		if (!this.timer) {
			this.timer = setTimeout(this.send, this.queueTime)
		}
		if (this.messages.push(message) >= this.batchSize) {
			return this.send()
		}
		return true
	}

	MessageBuffer.prototype.flush = function () {
		if (this.messages.length > 0) {
			this.send()
		}
	}

	return MessageBuffer
}
