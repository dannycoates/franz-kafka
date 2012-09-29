module.exports = function (
	inherits,
	State) {

	function OffsetsBody(bytes) {
		State.call(this, bytes)
	}
	inherits(OffsetsBody, State)

	OffsetsBody.prototype.parse = function () {
		console.assert(this.complete())
		var offsets = []
		var count = this.buffer.readUInt32BE(0)
		for (var i = 0; i < count; i++) {
			var high = 4 + (i * 8)
			var low = 4 + (i * 8) + 4
			offsets.push([this.buffer.readUInt32BE(high),this.buffer.readUInt32BE(low)])
		}
		return offsets
	}

	OffsetsBody.prototype.next = function () {
		return this.parse()
	}

	return OffsetsBody
}
