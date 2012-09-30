module.exports = function (
	RequestHeader) {

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
		var high = 0
		var low = this.time & 4294967295 //0xFFFFFFFF
		if (this.time > 4294967295) {
			high = Math.floor((this.time - low) / 4294967295)
		}
		else if (this.time < 0) { // for the -1 and -2 special cases
			high = 4294967295
			low = 4294967296 + this.time
		}
		payload.writeUInt32BE(high, 0)
		payload.writeUInt32BE(low, 4)
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