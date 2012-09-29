module.exports = function (
	RequestHeader,
	Message) {

	function ProduceRequest() {
		this.header = null
		this.messages = []
	}

	function messageToBuffer(m) { return m.toBuffer() }
	function sumLength(t, b) { return t + b.length }


	ProduceRequest.prototype._compress = function (stream, cb) {
		var messageBuffers = this.messages.map(messageToBuffer)
		var messagesLength = messageBuffers.reduce(sumLength, 0)
		var wrapper = new Message()
		wrapper.setData(
			Buffer.concat(messageBuffers, messagesLength),
			Message.compression.GZIP,
			function (err) {
				cb(err, wrapper.toBuffer())
			}
		)
	}

	ProduceRequest.prototype.serialize = function (stream) {
		var payload
		this._compress(
			stream,
			function (err, buffer) {
				this.header = new RequestHeader(buffer.length + 4, 0, 'test')
				this.header.serialize(stream)

				var mlen = new Buffer(4)
				mlen.writeUInt32BE(buffer.length, 0)
				stream.write(mlen)

				stream.write(buffer)
			}
		)
	}

	return ProduceRequest
}
