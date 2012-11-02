module.exports = function (
	inherits,
	EventEmitter,
	MessageBuffer) {

	function Topic(name, connector, compression, batchSize, queueTime) {
		this.offset = 0
		this.name = name || ''
		this.partitions = []
		this.connector = connector
		this.ready = true
		this.compression = 0
		this.messages = new MessageBuffer(this, batchSize, queueTime, connector)
		EventEmitter.call(this)
	}
	inherits(Topic, EventEmitter)

	Topic.prototype.parseMessages = function(err, length, messages) {
		if (err) {
			return this.emit('error', err)
		}
		var self = this
		this.offset += length
		for (var i = 0; i < messages.length; i++) {
			//XXX do we need to preserve the order?
			messages[i].unpack(
				function (payloads) {
					payloads.forEach(
						function (data) {
							self.emit('message', data)
						}
					)
				}
			)
		}
	}

	Topic.prototype.setReady = function (ready) {
		if(ready && !this.ready) {
			this.emit('ready')
		}
		this.ready = ready
	}

	Topic.prototype.publish = function (messages) {
		var self = this
		if (!Array.isArray(messages)) {
			messages = [messages]
		}
		return messages.every(
			function (m) {
				return self.messages.push(m)
			}
		)
	}

	Topic.prototype.consume = function (interval) { //TODO: starting offset?
		this.connector.consume(this, interval)
	}

	return Topic
}
