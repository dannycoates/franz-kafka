module.exports = function () {

	function RequestHeader(payloadLength, type, topic, partition) {
		type = type || 0
		topic = topic || ""
		partition = partition || 0
		payloadLength = payloadLength || 0
		var topicLength = Buffer.byteLength(topic)
		var length = payloadLength + topicLength + 8
		this.header = new Buffer(topicLength + 12)
		this.header.writeUInt32BE(length, 0)
		this.header.writeUInt16BE(type, 4)
		this.header.writeUInt16BE(topicLength, 6)
		this.header.write(topic, 8)
		this.header.writeUInt32BE(partition, this.header.length - 4)
		this.type = type
	}

	RequestHeader.prototype.serialize = function (stream) {
		return stream.write(this.header)
	}

	return RequestHeader
}
