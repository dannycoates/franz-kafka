module.exports = function (
	inherits,
	EventEmitter) {

	function Topic(name, kafka) {
		this.offset = 0
		this.name = name || ''
		this.partitions = []
		this.kafka = kafka
		this.interval = null
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

	Topic.prototype.publish = function (messages) {
		this.kafka.publish(this, messages)
	}

	Topic.prototype.consume = function (interval) { //TODO: starting offset?
		var self = this
		this.kafka.connectConsumer(
			function () {
				clearInterval(self.interval)
				self.interval = setInterval(
					function () {
						self.kafka.fetch(self)
					},
					interval
				)
			}
		)
	}

	return Topic
}
