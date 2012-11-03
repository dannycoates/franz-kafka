module.exports = function (
	State,
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
			var next = this.state.next()
			if (next === State.doneState) {
				this.done = true
				this.cb(
					this.state.error(),
					this.state.buffer.length,
					this.state.body()
				)
				break;
			}
			else {
				this.state = next
			}
		}
		return this.done
	}

	return Response
}
