module.exports = function (
	logger,
	inherits,
	EventEmitter,
	Client) {

	//TODO change to (id, options)
	function Broker(id, host, port, options) {
		this.id = id
		this.client = Client.nil
		this.reconnectAttempts = 0
		this.options = options || {}
		this.options.host = host
		this.options.port = port
		this.connector = this.connect.bind(this)
		EventEmitter.call(this)
	}
	inherits(Broker, EventEmitter)

	function exponentialBackoff(attempt) {
		return Math.floor(
			Math.random() * Math.pow(2, attempt) * 10
		)
	}

	Broker.prototype.connect = function () {
		var options = this.options
		logger.info(
			'connecting broker', this.id,
			'host', options.host,
			'port', options.port
		)
		this.client = new Client(this.id, options)
		this.client.once(
			'connect',
			function () {
				logger.info('broker connected', this.id)
				this.reconnectAttempts = 0
				this.emit('connect', this)
			}.bind(this)
		)
		this.client.once(
			'end',
			function () {
				this.reconnectAttempts++
				logger.info('broker ended', this.id, this.reconnectAttempts)
				setTimeout(
					this.connector,
					exponentialBackoff(this.reconnectAttempts)
				)
			}.bind(this)
		)
		this.client.on(
			'ready',
			function () {
				logger.info('broker ready', this.id)
				this.emit('ready', this)
			}.bind(this)
		)
	}

	Broker.prototype.isReady = function () {
		return this.client.ready
	}

	Broker.prototype.fetch = function (topic, partition, cb) {
		this.client.fetch(topic, partition, cb)
	}

	Broker.prototype.write = function (partition, messages, cb) {
		return this.client.write(partition.topic, messages, partition.id, cb)
	}

	Broker.prototype.drain = function (cb) {
		this.client.drain(cb)
	}

	Broker.nil = new Broker()

	return Broker
}
