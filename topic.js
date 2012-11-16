module.exports = function (
	logger,
	inherits,
	Stream,
	MessageBuffer,
	Partition,
	PartitionSet) {

	// A Topic is Readable/Writable Stream.
	// It's the main interaction point of the API.
	// Consuming is via the node ReadableStream API.
	// Producing is with the node WritableStream API.
	// API API API
	//
	// name: string
	// kafka: Kafka
	// options: {
	//   minFetchDelay: number (ms)
	//   maxFetchDelay: number (ms)
	//   maxFetchSize: number (bytes)
	//   compression: Message.compression (emum)
	//   batchSize: number (count)
	//   queueTime: number (ms)
	//   partitions: {
	//     consume: [string] (broker-partition:offset) ex. '0-0:123'
	//     produce: [string] (broker:partitionCount) ex. '0:5'
	//   }
	// }
	function Topic(name, kafka, options) {
		this.name = name || ''
		this.minFetchDelay = options.minFetchDelay
		this.maxFetchDelay = options.maxFetchDelay
		this.maxFetchSize = options.maxFetchSize
		this.maxMessageSize = options.maxMessageSize
		this.kafka = kafka
		this.partitions = new PartitionSet()
		if (options.partitions) {
			this.addWritablePartitions(options.partitions.produce)
			this.consumePartitions = options.partitions.consume
		}
		this.ready = true
		this.compression = options.compression
		this.readable = true
		this.writable = true
		this.encoding = null
		this.outgoingMessages = new MessageBuffer(
			this.partitions,
			options.batchSize,
			options.queueTime
		)
		this.outgoingMessages.on(
			'error',
			function (err) {
				this.error(err)
			}.bind(this)
		)
		this.bufferedMessages = []
		this.emitMessages = emitMessages.bind(this)
		Stream.call(this)
	}
	inherits(Topic, Stream)

	//emit end
	//emit close

	function emitMessages(payloads) {
		for (var i = 0; i < payloads.length; i++) {
			var data = payloads[i]
			if (this.encoding) {
				data = data.toString(this.encoding)
			}
			if (this.paused) {
				logger.info(
					'buffering', this.name,
					'length', this.bufferedMessages.length
				)
				this.bufferedMessages.push(data)
			}
			else {
				this.emit('data', data)
			}
		}
	}

	Topic.prototype.parseMessages = function(partition, messages) {
		this.emit('offset', partition.name(), partition.offset)
		for (var i = 0; i < messages.length; i++) {
			messages[i].unpack(this.emitMessages)
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
		//this.consumer.saveOffsets(this)
	}

	// Partitions

	Topic.prototype.partition = function (brokerId, partitionId) {
		var name = brokerId + '-' + partitionId
		var partition = this.partitions.get(name)
		if (!partition) {
			partition = new Partition(this, this.kafka.broker(brokerId), partitionId) // TODO options
			this.partitions.add(partition)
		}
		return partition
	}

	Topic.prototype.addWritablePartitions = function (partitionInfo) {
		if (!Array.isArray(partitionInfo)) {
			return
		}
		for (var i = 0; i < partitionInfo.length; i++) {
			var info = partitionInfo[i]
			var brokerPartitionCount = info.split(':')
			if (brokerPartitionCount.length === 2) {
				var brokerId = +brokerPartitionCount[0]
				var partitionCount = +brokerPartitionCount[1]
				for (var j = 0; j < partitionCount; j++) {
					var p = this.partition(brokerId, j)
					p.isWritable(true)
				}
			}
		}
	}

	// TODO a way to add/remove readablePartitions

	// Readable Stream

	Topic.prototype.error = function (err) {
		if (!this.paused) {
			this.pause()
		}
		logger.info('topic', this.name, 'error', err.message)
		this.emit('error', err)
	}

	Topic.prototype.pause = function () {
		logger.info('pause', this.name)
		this.paused = true
		this.partitions.pause()
	}

	Topic.prototype.resume = function () {
		//TODO first resume setup
		logger.info('resume', this.name)
		this.paused = this._flushBufferedMessages()
		if (!this.paused) {
			this.partitions.resume()
		}
	}

	Topic.prototype.destroy = function () {
		this.partitions.stop()
	}

	Topic.prototype.setEncoding = function (encoding) {
		this.encoding = encoding
	}

	//Writable Stream

	//TODO figure out the new ready behaviour
	Topic.prototype.setReady = function (ready) {
		if(ready && !this.ready) {
			this.outgoingMessages.flush()
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
