module.exports = function (
	RequestHeader) {

	function OffsetsRequest() {
		this.header = new RequestHeader()
		this.time = []
		this.maxOffsets = 0
	}

	return OffsetsRequest
}