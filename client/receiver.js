module.exports = function (
	inherits,
	EventEmitter,
	State) {

	function Receiver(stream) {
		var self = this
		this.stream = stream
		this.stream.on(
			'readable',
			function () {
				self.read()
			}
		)
		this.stream.on(
			'end',
			function () {
				self.closed = true
			}
		)
		this.queue = []
		this.current = State.nil
		this.closed = false
		EventEmitter.call(this)
	}
	inherits(Receiver, EventEmitter)

	Receiver.prototype.isEmpty = function () {
		return this.queue.length === 0
	}

	Receiver.prototype.close = function (cb) {
		this.closed = true
		if (this.isEmpty()) {
			cb()
		}
		else {
			this.once('empty', cb)
		}
	}

	Receiver.prototype.open = function () {
		this.closed = false
	}

	Receiver.prototype.next = function () {
		if (this.isEmpty() && this.current !== State.nil) {
			this.emit('empty')
		}
		this.current = this.queue.shift() || State.nil
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
		if (response !== State.nil) {
			this.queue.push(response)
		}
		return true
	}

	return Receiver
}
