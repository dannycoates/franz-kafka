module.exports = function (
	RequestHeader,
	Message,
	State) {

	function ProduceRequest(topic, messages, partition, compression) {
		this.topic = topic || ""
		this.messages = messages || []
		this.partition = partition
		this.compression = compression
	}

	function messageToBuffer(m) { return m.toBuffer() }
	function sumLength(t, b) { return t + b.length }


	ProduceRequest.prototype._compress = function (cb) {
		var messageBuffers = this.messages.map(messageToBuffer)
		var messagesLength = messageBuffers.reduce(sumLength, 0)
		var wrapper = new Message()
		wrapper.setData(
			Buffer.concat(messageBuffers, messagesLength),
			this.compression,
			function (err) {
				cb(err, wrapper.toBuffer())
			}
		)
	}

	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// /                         REQUEST HEADER                        /
	// /                                                               /
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                         MESSAGES_LENGTH                       |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// /                                                               /
	// /                            MESSAGES                           /
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	//
	// MESSAGES_LENGTH = int32 // Length in bytes of the MESSAGES section
	// MESSAGES = Collection of MESSAGES
	ProduceRequest.prototype.serialize = function (stream, cb) {
		var self = this
		var payload
		this._compress(
			function (err, buffer) {
				var err = null
				var header = new RequestHeader(
					buffer.length + 4,
					RequestHeader.types.PRODUCE,
					self.topic,
					self.partition
				)
				try {
					header.serialize(stream)
					var mlen = new Buffer(4)
					mlen.writeUInt32BE(buffer.length, 0)
					stream.write(mlen)
					var written = stream.write(buffer)
				}
				catch (e) {
					err = e
				}
				cb(err, written)
			}
		)
	}

	ProduceRequest.prototype.response = function (cb) {
		cb()
		return State.done
	}

	return ProduceRequest
}
