module.exports = function (
	RequestHeader) {

	function ProduceRequest() {
		this.header = null
		this.messages = []
	}

	ProduceRequest.prototype.serialize = function (stream) {
		var messageBuffers = this.messages.map(function (m) { return m.toBuffer() })
		var messagesLength = messageBuffers.reduce(function (t, b) { return t + b.length }, 0)
		this.header = new RequestHeader(messagesLength + 4, 0, 'test')
		console.log("mlen " + messagesLength)
		console.log(this.header.header.toString('hex'))
		this.header.serialize(stream)
		var mlen = new Buffer(4)
		mlen.writeUInt32BE(messagesLength, 0)
		console.log(mlen.toString('hex'))
		stream.write(mlen)
		messageBuffers.forEach(function (b) { stream.write(b) })
	}

	return ProduceRequest
}
