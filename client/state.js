module.exports = function (inherits) {

	function State(bytes) {
		this.remainingBytes = bytes
		this.buffer = new Buffer(bytes)
	}

	function NullState() {
		State.call(this, 0)
	}
	inherits(NullState, State)
	NullState.prototype.read = function () { return false }

	State.nil = new NullState()
	State.done = State.nil //synonym for clarity

	State.prototype.complete = function () {
		return this.remainingBytes === 0
	}

	State.prototype.read = function (stream) {
		if (this.complete()) { return true }
		var data = stream.read(this.remainingBytes)
		if (!data) { return false }
		data.copy(this.buffer, this.buffer.length - this.remainingBytes)
		this.remainingBytes = this.remainingBytes - data.length
		return this.complete()
	}

	State.prototype.next = function () { return State.nil }

	State.prototype.toString = function () {
		return "State " + this.complete() + " " + this.buffer.toString('hex')
	}

	State.prototype.body = function () { return null }

	State.prototype.error = function () { return null }

	State.prototype.abort = function () {}

	return State
}
