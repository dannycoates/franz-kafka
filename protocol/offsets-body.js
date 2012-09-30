module.exports = function (
	inherits,
	State) {

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
		console.assert(this.complete())
		var offsets = []
		var count = this.buffer.readUInt32BE(0)
		for (var i = 0; i < count; i++) {
			var high = 4 + (i * 8)
			var low = 4 + (i * 8) + 4
			// its unlikely the offset will exceed 53 bits for a while.
			// for now we'll use doubles because that's what we have
			high = this.buffer.readUInt32BE(high)
			low = this.buffer.readUInt32BE(low)
			if (high > 2097151) { //2^21 - 1
				throw new Error("Offset too big")
			}
			var offset = (high * 4294967296) + low
			offsets.push(offset)
		}
		return offsets
	}

	OffsetsBody.prototype.next = function () {
		return this.parse()
	}

	return OffsetsBody
}
