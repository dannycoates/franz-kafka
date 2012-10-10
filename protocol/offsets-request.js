module.exports = function (
	RequestHeader,
	int53) {

	function OffsetsRequest() {
		this.header = null
		this.time = 0
		this.maxCount = 0
	}


	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// /                         REQUEST HEADER                        /
	// /                                                               /
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                              TIME                             |
	// |                                                               |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                     MAX_NUMBER (of OFFSETS)                   |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	//
	// TIME = int64 // Milliseconds since UNIX Epoch.
	//              // -1 = LATEST
	//              // -2 = EARLIEST
	// MAX_NUMBER = int32 // Return up to this many offsets
	OffsetsRequest.prototype.serialize =function (stream) {
		var payload = new Buffer(12)
		int53.writeInt64BE(this.time, payload)
		payload.writeUInt32BE(this.maxCount, 8)
		this.header = new RequestHeader(
			payload.length,
			RequestHeader.types.OFFSETS,
			"test")
		this.header.serialize(stream)
		return stream.write(payload)
	}

	return OffsetsRequest
}
