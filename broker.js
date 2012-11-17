module.exports = function (
	logger,
	inherits,
	EventEmitter,
	Client) {

	//TODO change to (id, options)
	function Broker(id, options) {
		this.id = id
		this.client = Client.nil
		this.reconnectAttempts = 0
		this.options = options
		this.connector = this.connect.bind(this)
		this.onClientEnd = clientEnd.bind(this)
		this.onClientReady = clientReady.bind(this)
		this.onClientConnect = clientConnect.bind(this)
		EventEmitter.call(this)
	}
	inherits(Broker, EventEmitter)

	Broker.prototype.connect = function () {
		var options = this.options
		logger.info(
			'connecting broker', this.id,
			'host', options.host,
			'port', options.port
		)
		this.client = new Client(this.id, options)

		this.client.once('connect', this.onClientConnect)
		this.client.once('end', this.onClientEnd)
		this.client.on('ready', this.onClientReady)

		this.reconnectTimer = null
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

	Broker.prototype.destroy = function () {
		clearTimeout(this.reconnectTimer)
		this.reconnectTimer = null
		this.client.removeListener('connect', this.onClientConnect)
		this.client.removeListener('end', this.onClientEnd)
		this.client.removeListener('ready', this.onClientReady)
		this.client.end()
		this.client = Client.nil
		this.emit('destroy')
	}

	function exponentialBackoff(attempt) {
		return Math.floor(
			Math.random() * Math.pow(2, attempt) * 10
		)
	}

	function clientConnect() {
		logger.info('broker connected', this.id)
		this.reconnectAttempts = 0
		this.emit('connect', this)
		this.emit('ready')
	}

	function clientEnd() {
		this.reconnectAttempts++
		logger.info(
			'broker ended', this.id,
			'reconnects', this.reconnectAttempts
		)
		this.client.removeListener('connect', this.onClientConnect)
		this.client.removeListener('end', this.onClientEnd)
		this.client.removeListener('ready', this.onClientReady)
		this.reconnectTimer = setTimeout(
			this.connector,
			exponentialBackoff(this.reconnectAttempts)
		)
	}

	function clientReady() {
		logger.info('broker ready', this.id)
		this.emit('ready')
	}

	Broker.nil = new Broker()

	return Broker
}
