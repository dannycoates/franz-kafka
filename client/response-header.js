module.exports = function (
	inherits,
	State) {

	function ResponseHeader(ResponseBody) {
		this.length = 0
		this.errno = 0
		this.ResponseBody = ResponseBody
		State.call(this, 6)
	}
	inherits(ResponseHeader, State)

	// ================  =====  ===================================================
	// ERROR_CODE        VALUE  DEFINITION
	// ================  =====  ===================================================
	// Unknown            -1    Unknown Error
	// NoError             0    Success
	// OffsetOutOfRange    1    Offset requested is no longer available on the server
	// InvalidMessage      2    A message you sent failed its checksum and is corrupt.
	// WrongPartition      3    You tried to access a partition that doesn't exist
	//                          (was not between 0 and (num_partitions - 1)).
	// InvalidFetchSize    4    The size you requested for fetching is smaller than
	//                          the message you're trying to fetch.
	// ================  =====  ===================================================

	function toError(errno) {
		var msg
		switch (errno) {
			case 1:
				msg = "OffsetOutOfRange"
				break;
			case 2:
				msg = "InvalidMessage"
				break;
			case 3:
				msg = "WrongPartition"
				break;
			case 4:
				msg = "InvalidFetchSize"
				break;
			default:
				msg = "Unknown Error " + errno
				break;
		}
		return new Error(msg)
	}

	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                        RESPONSE_LENGTH                        |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |         ERROR_CODE            |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

	// RESPONSE_LENGTH = int32 // Length in bytes of entire response (excluding this field)
	// ERROR_CODE = int16
	ResponseHeader.prototype.parse = function () {
		this.length = this.buffer.readUInt32BE(0)
		this.errno = this.buffer.readUInt16BE(4)
	}

	ResponseHeader.prototype.next = function () {
		this.parse()
		if (this.errno !== 0) {
			return State.done
		}
		return new this.ResponseBody(this.length - 2)
	}

	ResponseHeader.prototype.error = function () {
		return toError(this.errno)
	}

	return ResponseHeader
}
