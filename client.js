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
		this.readableSteam = new ReadableStream()
		this.readableSteam.wrap(this.connection)
		this.receiver = new Receiver(this.readableSteam)
	}
	inherits(Client, EventEmitter)

	function parseMessages(messages) {
		var self = this
		for (var i = 0; i < messages.length; i++) {
			//XXX do we need to preserve the order?
			messages[i].unpack(
				function (payloads) {
					payloads.forEach(
						function (data) {
							self.emit('message', data)
						}
					)
				}
			)
		}
	}

	Client.prototype.fetch = function (topic, offset, partition, maxSize) {
		var request = new FetchRequest(topic, offset, partition, maxSize)
		//TODO something with these return values
		request.serialize(this.connection)
		this.receiver.push(request, parseMessages.bind(this))
	}

	Client.prototype.produce = function (topic, messages, partition) {
		console.assert(messages)

		if (!Array.isArray(messages)) {
			messages = [messages]
		}
		var request = new ProduceRequest(topic, messages.map(Message.create), partition)
		request.serialize(this.connection)
	}

	Client.prototype.offsets = function (time, maxCount, cb) {
		var request = new OffsetsRequest()
		request.time = time
		request.maxCount = maxCount
		request.serialize(this.connection)
		this.receiver.push(request, cb)
	}

	return Client
}
