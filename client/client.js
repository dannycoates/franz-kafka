module.exports = function (
	logger,
	net,
	inherits,
	EventEmitter,
	ReadableStream,
	Message,
	Receiver,
	FetchRequest,
	ProduceRequest,
	OffsetsRequest
) {
	function Client(id, options) {
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
				if (!self.ready) { //TODO: why is connection.drain so frequent?
					self.ready = true
					self.emit('ready')
				}
			}
		)
		this.id = id
		this.ready = true
		this.readableSteam = new ReadableStream()
		this.readableSteam.wrap(this.connection)
		this.receiver = new Receiver(this.readableSteam)
		EventEmitter.call(this)
	}
	inherits(Client, EventEmitter)

	Client.prototype.drain = function (cb) {
		var self = this
		logger.info('draining', this.id)
		this.receiver.close(
			function () {
				logger.info('drained', self.id)
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
			function (err, written) {
				if (err) {
					return cb(err)
				}
				if (!written) {
					self.ready = false
				}
				self.receiver.push(request, cb)
			}
		)
		return this.ready
	}

	// cb: function (err, length, messages) {}
	Client.prototype.fetch = function (name, partition, maxSize, cb) {
		logger.info(
			'fetching', name,
			'broker', this.id,
			'partition', partition.id
		)
		return this._send(
			new FetchRequest(
				name,
				partition.offset,
				partition.id,
				maxSize
			),
			cb
		)
	}

	// topic: a Topic object
	// messages: array of: string, Buffer, Message
	// partition: number
	Client.prototype.publish = function (topic, messages, partitionId, cb) {
		logger.info(
			'publishing', topic.name,
			'messages', messages.length,
			'broker', this.id,
			'partition', partitionId
		)
		return this._send(
			new ProduceRequest(
				topic.name,
				messages.map(Message.create),
				partitionId,
				topic.compression
			),
			cb
		)
	}

	Client.prototype.offsets = function (time, maxCount, cb) {
		logger.info(
			'offsets', time,
			'broker', this.id
		)
		return this._send(new OffsetsRequest(time, maxCount), cb)
	}

	Client.compression = Message.compression

	return Client
}
