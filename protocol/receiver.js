module.exports = function (
	State) {

	function Receiver(stream) {
		var self = this
		this.stream = stream
		this.stream.on('readable',
			function () {
				self.read()
			}
		)
		this.stream.on('end',
			function () {
				self.closed = true
			}
		)
		this.queue = []
		this.current = State.nullState
		this.closed = false
	}

	Receiver.prototype.next = function () {
		this.current = this.queue.shift() || State.nullState
	}

	Receiver.prototype.read = function () {
		if (this.current.complete()) {
			this.next()
		}
		while (this.current.read(this.stream)) {
			this.next()
		}
	}

	Receiver.prototype.push = function (request, cb) {
		if (this.closed) { return false } // or something
		var response = request.response(cb)
		if (response !== State.nullState) {
			this.queue.push(response)
		}
		return true
	}

	return Receiver
}
