module.exports = function (
	inherits,
	State,
	int53) {

	function OffsetsBody(bytes) {
		State.call(this, bytes)
	}
	inherits(OffsetsBody, State)

	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// /                         RESPONSE HEADER                       /
	// /                                                               /
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                         NUMBER_OFFSETS                        |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// /                       OFFSETS (0 or more)                     /
	// /                                                               /
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

	// NUMBER_OFFSETS = int32 // How many offsets are being returned
	// OFFSETS = int64[] // List of offsets
	OffsetsBody.prototype.parse = function () {
		var offsets = []
		var count = this.buffer.readUInt32BE(0)
		for (var i = 0; i < count; i++) {
			var offset = 4 + (i * 8)
			// its unlikely the offset will exceed 53 bits for a while.
			// for now we'll use doubles because that's what we have
			offsets.push(int53.readUInt64BE(this.buffer, offset))
		}
		return offsets
	}

	OffsetsBody.prototype.body = function () {
		return this.parse()
	}

	return OffsetsBody
}
