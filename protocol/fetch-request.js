module.exports = function (
	RequestHeader) {

	function FetchRequest() {
		this.header = null
		this.offset = []
		this.maxSize = 0
	}

	FetchRequest.prototype.serialize = function (stream) {
		var payload = new Buffer(12)
		payload.writeUInt32BE(this.offset[0], 0)
		payload.writeUInt32BE(this.offset[1], 4)
		payload.writeUInt32BE(this.maxSize, 8)
		this.header = new RequestHeader(payload.length, 1, "test")
		this.header.serialize(stream)
		return stream.write(payload)
	}

	return FetchRequest
}
