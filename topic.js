module.exports = function (
	logger,
	inherits,
	Stream,
	MessageBuffer) {

	function Topic(name, producer, consumer, options) {
		this.name = name || ''
		this.minFetchDelay = options.minFetchDelay
		this.maxFetchDelay = options.maxFetchDelay
		this.maxFetchSize = options.maxFetchSize
		this.producer = producer
		this.consumer = consumer
		this.partitions = options.partitions
		this.ready = true
		this.compression = options.compression
		this.readable = true
		this.writable = true
		this.encoding = null
		this.outgoingMessages = new MessageBuffer(
			this,
			options.batchSize,
			options.queueTime,
			this.producer
		)
		this.bufferedMessages = []
		Stream.call(this)
	}
	inherits(Topic, Stream)

	//emit end
	//emit error
	//emit close

	Topic.prototype.parseMessages = function(messages) {
		var self = this
		for (var i = 0; i < messages.length; i++) {
			messages[i].unpack(
				function (payloads) {
					payloads.forEach(
						function (data) {
							if (self.encoding) {
								data = data.toString(self.encoding)
							}
							if (self.paused) {
								logger.info(
									'buffering', self.name,
									'length', self.bufferedMessages.length
								)
								self.bufferedMessages.push(data)
							}
							else {
								self.emit('data', data)
							}
						}
					)
				}
			)
		}
	}

	Topic.prototype._flushBufferedMessages = function () {
		this.paused = false
		while(!this.paused && this.bufferedMessages.length > 0) {
			this.emit('data', this.bufferedMessages.shift())
		}
		logger.info(
			'flushed', this.name,
			'remaining', this.bufferedMessages.length,
			'paused', this.paused
		)
		return this.paused || this.bufferedMessages.length > 0
	}

	Topic.prototype.saveOffsets = function () {
		this.consumer.saveOffsets(this)
	}

	// Readable Stream

	Topic.prototype.error = function (err) {
		if (!this.paused) {
			this.pause()
		}
		this.emit('error', err)
	}

	Topic.prototype.pause = function () {
		logger.info('pause', this.name)
		this.paused = true
		this.consumer.pause(this)
	}

	Topic.prototype.resume = function () {
		logger.info('resume', this.name)
		this.paused = this._flushBufferedMessages()
		if (!this.paused) {
			this.consumer.resume(this)
		}
	}

	Topic.prototype.destroy = function () {
		this.consumer.stop(this)
	}

	Topic.prototype.setEncoding = function (encoding) {
		this.encoding = encoding
	}

	//Writable Stream

	Topic.prototype.setReady = function (ready) {
		if(ready && !this.ready) {
			this.emit('drain')
		}
		this.ready = ready
	}

	Topic.prototype.write = function (data, encoding) {
		if(!Buffer.isBuffer(data)) {
			encoding = encoding || 'utf8'
			data = new Buffer(data, encoding)
		}
		return this.outgoingMessages.push(data)
	}

	Topic.prototype.end = function (data, encoding) {
		this.write(data, encoding)
	}

	Topic.prototype.destroySoon = function () {
		this.destroy()
	}

	return Topic
}
