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
				logger.info('client connect')
				self.readableSteam = new ReadableStream()
				self.readableSteam.wrap(self.connection)
				self.receiver = new Receiver(self.readableSteam)
				self.ready = true
				self.emit('connect')
			}
		)
		this.connection.on(
			'end',
			function () {
				logger.info('client end')
				self.ready = false
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
		this.connection.on(
			'error',
			function (err) {
				//logger.info('client error', err)
			}
		)
		this.connection.on(
			'close',
			function (hadError) {
				logger.info('client closed with error:', hadError)
				self.emit('end')
			}
		)
		this.id = id
		this.ready = false
		this.readableSteam = null
		this.receiver = null
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
					this.ready = false
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
	Client.prototype.write = function (topic, messages, partitionId, cb) {
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
				topic.compression,
				topic.maxMessageSize
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
