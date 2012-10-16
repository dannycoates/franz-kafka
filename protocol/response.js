module.exports = function (
	ResponseHeader) {

	function Response(ResponseBody, cb) {
		this.state = new ResponseHeader(ResponseBody)
		this.cb = cb
		this.done = false
	}

	Response.prototype.complete = function () {
		return this.done
	}

	Response.prototype.read = function (stream) {
		while (this.state.read(stream)) {
			var result = this.state.next()
			if (Array.isArray(result)) { // TODO: better
				this.done = true
				this.cb(this.state.buffer.length, result)
				break;
			}
			else {
				this.state = result
			}
		}
		return this.done
	}

	return Response
}
