module.exports = function (
	inherits,
	RequestHeader,
	Message,
	State) {

	function ProduceRequest(topic, partitionId, messages) {
		this.topic = topic
		this.partitionId = partitionId
		this.messages = messages || []
	}

	function messageToBuffer(m) { return m.toBuffer() }
	function sumLength(t, b) { return t + b.length }


	ProduceRequest.prototype._compress = function (cb) {
		var messageBuffers = this.messages.map(messageToBuffer)
		var messagesLength = messageBuffers.reduce(sumLength, 0)
		var payload = Buffer.concat(messageBuffers, messagesLength)

		if (this.topic.compression === Message.compression.NONE) {
			cb(null, payload)
		}
		else {
			var wrapper = new Message()
			wrapper.setData(
				payload,
				this.topic.compression,
				function (err) {
					cb(err, wrapper.toBuffer())
				}
			)
		}
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
		this._compress(writeRequest.bind(this, stream, cb))
	}

	function writeRequest(stream, cb, err, buffer) {
		if (err) {
			return cb(err)
		}
		if (this.topic.compression !== Message.compression.NONE &&
		    buffer.length > this.topic.maxMessageSize) {
			return cb(new ProduceError("message too big", buffer.length))
		}
		var header = new RequestHeader(
			buffer.length + 4,
			RequestHeader.types.PRODUCE,
			this.topic.name,
			this.partitionId
		)
		try {
			var written = header.serialize(stream)
			var mlen = new Buffer(4)
			mlen.writeUInt32BE(buffer.length, 0)
			written = stream.write(mlen) && written
			written = stream.write(buffer) && written
		}
		catch (e) {
			err = e
		}
		cb(err, written)
	}

	ProduceRequest.prototype.response = function (cb) {
		cb()
		return State.done
	}

	function ProduceError(message, length) {
		this.message = message
		this.length = length
		Error.call(this)
	}
	inherits(ProduceError, Error)
	ProduceError.prototype.name = 'Produce Error'

	return ProduceRequest
}
