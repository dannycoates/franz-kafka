module.exports = function (
	RequestHeader) {

	function FetchRequest() {
		this.header = null
		this.offset = 0
		this.maxSize = 0
	}

	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// /                         REQUEST HEADER                        /
	// /                                                               /
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                             OFFSET                            |
	// |                                                               |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                            MAX_SIZE                           |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	//
	// REQUEST_HEADER = See REQUEST_HEADER above
	// OFFSET   = int64 // Offset in topic and partition to start from
	// MAX_SIZE = int32 // MAX_SIZE of the message set to return
	FetchRequest.prototype.serialize = function (stream) {
		var payload = new Buffer(12)
		var high = 0
		var low = this.offset & 4294967295 //0xFFFFFFFF
		if (this.offset > 4294967295) {
			high = Math.floor((this.offset - low) / 4294967295)
		}
		payload.writeUInt32BE(high, 0)
		payload.writeUInt32BE(low, 4)
		payload.writeUInt32BE(this.maxSize, 8)
		this.header = new RequestHeader(
			payload.length,
			RequestHeader.types.FETCH,
			"test")
		this.header.serialize(stream)
		return stream.write(payload)
	}

	return FetchRequest
}
