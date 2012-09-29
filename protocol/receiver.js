module.exports = function (
	NullState,
	Response) {

	var nullState = new NullState()

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
		this.current = nullState
		this.closed = false
	}

	Receiver.prototype.next = function () {
		this.current = this.queue.shift() || nullState
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
		this.queue.push(new Response(request, cb))
		return true
	}

	return Receiver
}
