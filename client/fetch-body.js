module.exports = function (
	inherits,
	State,
	Message) {

	function FetchBody(bytes) {
		this.bytesParsed = 0
		this.lastMessageLength = 0
		State.call(this, bytes)
	}
	inherits(FetchBody, State)

	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// /                          RESPONSE HEADER                      /
	// /                                                               /
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// /                        MESSAGES (0 or more)                   /
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	FetchBody.prototype.parse = function () {
		var messages = []
		this.bytesParsed = 0
		var blen = this.buffer.length
		while (this.bytesParsed < blen) {
			this.lastMessageLength = this.buffer.readUInt32BE(this.bytesParsed)
			var end = this.bytesParsed + this.lastMessageLength + 4
			if (end > blen) {
				// the remainder of the buffer is a partial message
				break;
			}
			messages.push(Message.parse(this.buffer.slice(this.bytesParsed, end)))
			this.bytesParsed += (this.lastMessageLength + 4)
		}
		return messages
	}

	FetchBody.prototype.body = function () {
		return this.parse()
	}

	FetchBody.prototype.error = function () {
		var err = null
		if (this.buffer.length > 0 && this.bytesParsed === 0) {
			err = new FetchError("message larger than buffer", this.lastMessageLength)
		}
		return err
	}

	function FetchError(message, messageLength) {
		this.message = message
		this.messageLength = messageLength
		Error.call(this)
	}
	inherits(FetchError, Error)
	FetchError.prototype.name = 'Fetch Error'

	return FetchBody
}
