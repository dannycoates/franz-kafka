module.exports = function (
	logger,
	State,
	ResponseHeader
) {
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
			if (next === State.done) {
				this.done = true
				var body = this.state.body()
				logger.info(
					'response', this.state.constructor.name,
					'length', this.state.buffer.length,
					'parsed', this.state.bytesParsed
				)
				this.cb(
					this.state.error(),
					this.state.bytesParsed,
					body
				)
				break;
			}
			else {
				this.state = next
			}
		}
		return this.done
	}

	Response.prototype.abort = function () {
		this.done = true
		this.cb(new Error('Response aborted'))
	}

	return Response
}
