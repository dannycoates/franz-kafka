module.exports = function () {

	function State(bytes) {
		this.remainingBytes = bytes
		this.buffer = new Buffer(bytes)
	}

	State.prototype.complete = function () {
		return this.remainingBytes === 0
	}

	State.prototype.read = function (stream) {
		if (this.complete()) { return true }
		var data = stream.read(this.remainingBytes)
		if (!data) { return false }
		console.assert(data.length <= this.remainingBytes)
		data.copy(this.buffer, this.buffer.length - this.remainingBytes)
		this.remainingBytes = this.remainingBytes - data.length
		return this.complete()
	}

	State.prototype.next = function () { return this }

	State.prototype.toString = function () {
		return "State " + this.complete() + " " + this.buffer.toString('hex')
	}

	return State
}
