module.exports = function (
	logger,
	inherits,
	EventEmitter,
	State) {

	function Receiver(stream) {
		this.stream = stream
		this.queue = []
		this.current = State.nil
		this.closed = false

		this.onStreamReadable = streamReadable.bind(this)
		this.onStreamEnd = streamEnd.bind(this)
		this.onStreamError = streamError.bind(this)

		this.stream.on('readable', this.onStreamReadable)
		this.stream.on('end', this.onStreamEnd)
		this.stream.on('error', this.onStreamError)

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

	function streamReadable() {
		this.read()
	}

	function streamEnd() {
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
		this.stream.removeListener('readable', this.onStreamReadable)
		this.stream.removeListener('end', this.onStreamEnd)
		this.stream.removeListener('error', this.onStreamError)
	}

	function streamError(err) {
		logger.info('receiver error', err.message)
	}

	return Receiver
}
