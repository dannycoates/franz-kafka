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

	Topic.prototype.parseMessages = function(length, messages) {
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

	Topic.prototype.consume = function (interval) {
		this.kafka.consume(this, interval)
	}

	return Topic
}