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
		this.id = id
		this.ready = false
		this.readableSteam = null
		this.receiver = null

		this.connection = net.connect(options)
		this.connection.setKeepAlive(true, 1000 * 60 * 5)
		this.onConnectionConnect = connectionConnect.bind(this)
		this.onConnectionEnd = connectionEnd.bind(this)
		this.onConnectionDrain = connectionDrain.bind(this)
		this.onConnectionError = connectionError.bind(this)
		this.onConnectionClose = connectionClose.bind(this)

		this.connection.on('connect', this.onConnectionConnect)
		this.connection.on('end', this.onConnectionEnd)
		this.connection.on('drain', this.onConnectionDrain)
		this.connection.on('error', this.onConnectionError)
		this.connection.on('close', this.onConnectionClose)

		EventEmitter.call(this)
	}
	inherits(Client, EventEmitter)

	Client.prototype.drain = function (cb) {
		logger.info('draining', this.id)
		this.receiver.close(
			function () {
				logger.info('drained', this.id)
				// XXX is reopening correct here?
				this.receiver.open()
				cb()
			}.bind(this)
		)
	}

	Client.prototype.end = function () {
		this.connection.end()
	}

	Client.prototype._send = function (request, cb) {
		request.serialize(
			this.connection,
			afterSend.bind(this, request, cb)
		)
		return this.ready
	}

	function afterSend(request, cb, err, written) {
		if (err) {
			this.ready = false
			return cb(err)
		}
		if (!written) {
			logger.info('connection', 'wait')
			this.ready = false
		}
		this.receiver.push(request, cb)
	}

	// cb: function (err, length, messages) {}
	Client.prototype.fetch = function (topic, partition, cb) {
		logger.info(
			'fetching', topic.name,
			'broker', this.id,
			'partition', partition.id,
			'offset', partition.offset
		)
		return this._send(new FetchRequest(topic, partition), cb)
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
				topic,
				partitionId,
				messages.map(Message.create)
			),
			cb
		)
	}

	Client.prototype.offsets = function (topic, partition, time, maxCount, cb) {
		logger.info(
			'offsets', time,
			'topic', topic.name,
			'broker', this.id,
			'partition', partition.id
		)
		return this._send(new OffsetsRequest(topic, partition, time, maxCount), cb)
	}

	Client.compression = Message.compression

	Client.nil = { ready: false }

	function connectionConnect() {
		logger.info('client connect')
		this.readableSteam = new ReadableStream()
		this.readableSteam.wrap(this.connection)
		this.receiver = new Receiver(this.readableSteam)
		this.ready = true
		this.emit('connect')
	}

	function connectionEnd() {
		logger.info('client end')
		this.ready = false
		this.emit('end')
	}

	function connectionDrain() {
		if (!this.ready) { //TODO: why is connection.drain so frequent?
			this.ready = true
			this.emit('ready')
		}
	}

	function connectionError(err) {
		logger.info('client error', err.message)
	}

	function connectionClose(hadError) {
		logger.info('client closed. with error', hadError)
		this.connection.removeListener('connect', this.onConnectionConnect)
		this.connection.removeListener('end', this.onConnectionEnd)
		this.connection.removeListener('drain', this.onConnectionDrain)
		this.connection.removeListener('error', this.onConnectionError)
		this.connection.removeListener('close', this.onConnectionClose)
		this.emit('end')
	}

	return Client
}
