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

	ResponseHeader.prototype.parse = function () {
		console.assert(this.complete())
		this.length = this.buffer.readUInt32BE(0)
		this.errno = this.buffer.readUInt16BE(4)
	}

	ResponseHeader.prototype.next = function () {
		this.parse()
		return new this.ResponseBody(this.length - 2)
	}

	return ResponseHeader
}
