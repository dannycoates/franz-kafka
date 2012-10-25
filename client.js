module.exports = function (
	net,
	inherits,
	EventEmitter,
	ReadableStream,
	Message,
	Receiver,
	FetchRequest,
	ProduceRequest,
	OffsetsRequest) {

	function Client(options) {
		var self = this
		this.connection = net.connect(options)
		this.connection.on(
			'connect',
			function () {
				self.emit('connect')
			}
		)
		this.connection.on(
			'end',
			function () {
				self.emit('end')
				self.connection = null
			}
		)
		this.connection.on(
			'drain',
			function () {
				self.ready = true
				self.emit('ready')
			}
		)
		this.ready = true
		this.readableSteam = new ReadableStream()
		this.readableSteam.wrap(this.connection)
		this.receiver = new Receiver(this.readableSteam)
	}
	inherits(Client, EventEmitter)

	Client.prototype._send = function (request, cb) {
		var self = this
		request.serialize(
			this.connection,
			function (written) {
				if (!written) {
					self.ready = false
				}
				self.receiver.push(request, cb)
			}
		)
		return this.ready
	}

	Client.prototype.fetch = function (topic, maxSize) {
		return this._send(
			new FetchRequest(topic.name, topic.offset, topic.partition, maxSize),
			topic.parseMessages.bind(topic)
		)
	}

	Client.prototype.produce = function (topic, messages, partition) {
		console.assert(messages)

		if (!Array.isArray(messages)) {
			messages = [messages]
		}
		return this._send(
			new ProduceRequest(
				topic.name,
				messages.map(Message.create),
				partition
			)
		)
	}

	Client.prototype.offsets = function (time, maxCount, cb) {
		return this._send(new OffsetsRequest(time, maxCount), cb)
	}

	return Client
}
