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
		EventEmitter.call(this)
	}
	inherits(Client, EventEmitter)

	Client.prototype.drain = function (cb) {
		var self = this
		this.receiver.close(
			function () {
				// XXX is reopening correct here?
				self.receiver.open()
				cb()
			}
		)
	}

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

	// cb: function (err, length, messages) {}
	Client.prototype.fetch = function (topic, partition, maxSize, cb) {
		return this._send(
			new FetchRequest(
				topic.name,
				topic.offset,
				partition,
				maxSize
			),
			cb
		)
	}

	// topic: a Topic object
	// messages: array of: string, Buffer, Message
	// partition: number
	Client.prototype.publish = function (topic, messages, partition) {
		//console.log('publish ' + topic.name + partition)
		return this._send(
			new ProduceRequest(
				topic.name,
				messages.map(Message.create),
				partition,
				topic.compression
			)
		)
	}

	Client.prototype.offsets = function (time, maxCount, cb) {
		return this._send(new OffsetsRequest(time, maxCount), cb)
	}

	Client.compression = Message.compression

	return Client
}
