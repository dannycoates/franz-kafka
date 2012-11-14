module.exports = function (
	logger,
	inherits,
	EventEmitter,
	State) {

	function Receiver(stream) {
		this.stream = stream
		this.stream.on(
			'readable',
			function () {
				this.read()
			}.bind(this)
		)
		this.stream.on(
			'end',
			function () {
				logger.info(
					'receiver', 'ended',
					'queue', this.queue.length
				)
				while (this.queue.length > 0) {
					this.current.abort()
					this.next()
				}
				this.current.abort()
				this.closed = true
			}.bind(this)
		)
		this.stream.on(
			'error',
			function (err) {
				logger.info('receiver error', err.message)
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
		if (this.closed) {
			cb(new Error('receiver closed'))
			return false
		}
		var response = request.response(cb)
		if (response !== State.nil) {
			this.queue.push(response)
		}
		return true
	}

	return Receiver
}
