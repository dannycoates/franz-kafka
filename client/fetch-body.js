module.exports = function (
	inherits,
	State,
	Message) {

	function FetchBody(bytes) {
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
		console.assert(this.complete())
		var messages = []
		var offset = 0
		while (offset < this.buffer.length) {
			var len = this.buffer.readUInt32BE(offset)
			messages.push(Message.parse(this.buffer.slice(offset, offset + len + 4)))
			offset += (len + 4)
		}
		return messages
	}

	FetchBody.prototype.body = function () {
		return this.parse()
	}

	return FetchBody
}
