module.exports = function (
	inherits,
	State,
	Message) {

	function FetchBody(bytes) {
		State.call(this, bytes)
		this.bytesParsed = 0
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
			var len = this.buffer.readUInt32BE(this.bytesParsed)
			var end = this.bytesParsed + len + 4
			if (end > blen) {
				// the remainder of the buffer is a partial message
				break;
			}
			messages.push(Message.parse(this.buffer.slice(this.bytesParsed, end)))
			this.bytesParsed += (len + 4)
		}
		return messages
	}

	FetchBody.prototype.body = function () {
		return this.parse()
	}

	return FetchBody
}
