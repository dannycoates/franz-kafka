module.exports = function (
	inherits,
	Stream,
	MessageBuffer) {

	function Topic(name, producer, consumer, options) {
		this.name = name || ''
		this.interval = options.interval
		this.producer = producer
		this.consumer = consumer
		this.partitions = options.partitions
		this.ready = true
		this.compression = options.compression
		this.readable = true
		this.writable = true
		this.encoding = null
		this.messages = new MessageBuffer(
			this,
			options.batchSize,
			options.queueTime,
			this.producer
		)
		Stream.call(this)
	}
	inherits(Topic, Stream)

	//emit end
	//emit error
	//emit close

	Topic.prototype.parseMessages = function(messages) {
		var self = this
		for (var i = 0; i < messages.length; i++) {
			//XXX do we need to preserve the order?
			messages[i].unpack(
				function (payloads) {
					payloads.forEach(
						function (data) {
							if (self.encoding) {
								data = data.toString(self.encoding)
							}
							self.emit('data', data)
						}
					)
				}
			)
		}
	}

	// Readable Stream

	Topic.prototype.error = function (err) {
		this.emit('error', err)
	}

	Topic.prototype.pause = function () {
		return this.consumer.pause(this)
	}

	Topic.prototype.resume = function () {
		return this.consumer.resume(this)
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
		return this.messages.push(data)
	}

	Topic.prototype.end = function (data, encoding) {
		this.write(data, encoding)
	}

	Topic.prototype.destroySoon = function () {
		this.destroy()
	}

	return Topic
}
