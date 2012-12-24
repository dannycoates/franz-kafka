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
		options = options || {}
		options.partitions = options.partitions || {}
		this.name = name || ''
		this.kafka = kafka
		this.encoding = null
		this.readable = true // required Stream property
		this.writable = true // required Stream property
		this.compression = options.compression
		this.minFetchDelay = options.minFetchDelay
		this.maxFetchDelay = options.maxFetchDelay
		this.maxFetchSize = options.maxFetchSize
		this.maxMessageSize = options.maxMessageSize
		this.bufferedMessages = []
		this.emitMessages = emitMessages.bind(this)

		this.partitions = new PartitionSet()
		this.onPartitionsReady = partitionsReady.bind(this)
		this.onPartitionMessages = this.parseMessages.bind(this)
		this.partitions.on('ready', this.onPartitionsReady)
		this.partitions.on('messages', this.onPartitionMessages)

		this.produceBuffer = new MessageBuffer(
			this.partitions,
			options.batchSize,
			options.queueTime
		)
		this.onError = this.error.bind(this)
		this.produceBuffer.on('error', this.onError)

		if (options.partitions.consume) {
			this.addReadablePartitions(options.partitions.consume)
		}

		if (options.partitions.produce) {
			this.addWritablePartitions(options.partitions.produce)
		}
		else {
			// create a partition to start with
			// TODO: which broker to pick?
			var b = this.kafka.broker()
			if (b) {
				this.addWritablePartitions([b.id + ':1'])
			}
		}

		Stream.call(this)
	}
	inherits(Topic, Stream)

	//TODO emit end
	//TODO emit close

	// a partition is ready for writing
	function partitionsReady() {
		if(this.produceBuffer.flush()) {
			logger.info('drain', this.name)
			this.emit('drain')
		}
	}

	// Emits a 'data' event for each message of a fetch response.
	// If the stream is paused, the message is added to bufferedMessages,
	// which is flushed when the stream is resumed.
	//
	// payloads: an array of Buffers
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

	// Emits the offset of the given messages, then unpacks and emits the
	// message data
	//
	// partition: a Partition object
	// messages: an Array of Message objects
	Topic.prototype.parseMessages = function (partition, messages) {
		this.emit('offset', partition.name, partition.offset)
		for (var i = 0; i < messages.length; i++) {
			messages[i].unpack(this.emitMessages)
		}
	}

	// Emits the messages that were buffered while the stream was paused
	//
	// returns: boolean pause state of this topic
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

	// Get or create a Partition object.
	//
	// name: string in the form of brokerId-partitionId
	//       ex. '12-6'
	// returns: a Partition object
	Topic.prototype.partition = function (name) {
		var partition = this.partitions.get(name)
		if (!partition) {
			var brokerPartition = name.split('-')
			var brokerId = +(brokerPartition[0])
			var partitionId = +(brokerPartition[1])
			var broker = this.kafka.broker(brokerId)
			if (broker) {
				partition = new Partition(this, broker, partitionId)
				this.partitions.add(partition)
			}
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
					var p = this.partition(brokerId + '-' + j)
					if (p) {
						p.isWritable(true)
					}
				}
			}
		}
	}

	Topic.prototype.addReadablePartitions = function (partitionInfo) {
		if (!Array.isArray(partitionInfo)) {
			return
		}
		for (var i = 0; i < partitionInfo.length; i++) {
			var info = partitionInfo[i]
			var nameOffset = info.split(':')
			var name = nameOffset[0]

			var partition = this.partition(name)
			if (partition) {
				partition.isReadable(true)
				if (nameOffset.length === 2) {
					var offset = +nameOffset[1]
					partition.offset = offset
				}
			}
		}
	}

	Topic.prototype.error = function (err) {
		if (!this.paused) {
			this.pause()
		}
		logger.info('topic', this.name, 'error', err.message)
		this.emit('error', err)
		return false
	}

	Topic.prototype.resetConsumer = function (cb) {
		this.partitions.stopConsuming()
		this.partitions.drain(cb)
	}

	// Readable Stream

	Topic.prototype.pause = function () {
		logger.info('pause', this.name)
		this.paused = true
		this.partitions.pause()
	}

	Topic.prototype.resume = function () {
		logger.info('resume', this.name)
		this.kafka.register(this)
		this.paused = this._flushBufferedMessages()
		if (!this.paused) {
			this.partitions.resume()
		}
	}

	Topic.prototype.destroy = function () {
		this.resetConsumer(function () {})
	}

	Topic.prototype.setEncoding = function (encoding) {
		this.encoding = encoding
	}

	//Writable Stream

	Topic.prototype.write = function (data, encoding) {
		if(!Buffer.isBuffer(data)) {
			encoding = encoding || 'utf8'
			data = new Buffer(data, encoding)
		}
		if (data.length > this.maxMessageSize) {
			return this.error(new Error("message too big"))
		}
		return this.produceBuffer.push(data)
	}

	Topic.prototype.end = function (data, encoding) {
		this.write(data, encoding)
	}

	Topic.prototype.destroySoon = function () {
		this.destroy()
	}

	return Topic
}
