module.exports = function (logger, inherits, RingList) {

	function Producer() {
		RingList.call(this)
	}
	inherits(Producer, RingList)

	function isReadyAndWritable(p) { return p.isReady() && p.isWritable() }

	Producer.prototype.isReady = function () {
		return this.items.some(isReadyAndWritable)
	}

	Producer.prototype.write = function (messages, cb) {
		console.assert(this.isReady()) // TODO
		this.next(isReadyAndWritable).write(messages, cb)
	}

	return Producer
}
