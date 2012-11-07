module.exports = function () {

	function RequestHeader(payloadLength, type, topic, partitionId) {
		type = type || 0
		topic = topic || ""
		partitionId = partitionId || 0
		payloadLength = payloadLength || 0
		var topicLength = Buffer.byteLength(topic)
		var length = payloadLength + topicLength + 8
		this.header = new Buffer(topicLength + 12)
		this.header.writeUInt32BE(length, 0)
		this.header.writeUInt16BE(type, 4)
		this.header.writeUInt16BE(topicLength, 6)
		this.header.write(topic, 8)
		this.header.writeUInt32BE(partitionId, this.header.length - 4)
	}

	RequestHeader.types = {
		PRODUCE: 0,
		FETCH: 1,
		MULTIFETCH: 2,
		MULTIPRODUCE: 3,
		OFFSETS: 4
	}


	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                       REQUEST_LENGTH                          |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |         REQUEST_TYPE          |        TOPIC_LENGTH           |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// /                                                               /
	// /                    TOPIC (variable length)                    /
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                           PARTITION                           |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	//
	// REQUEST_LENGTH = int32 // Length in bytes of entire request (excluding this field)
	// REQUEST_TYPE   = int16 // See table below
	// TOPIC_LENGTH   = int16 // Length in bytes of the topic name
	//
	// TOPIC = String // Topic name, ASCII, not null terminated
	//                // This becomes the name of a directory on the broker, so no
	//                // chars that would be illegal on the filesystem.
	//
	// PARTITION = int32 // Partition to act on. Number of available partitions is
	//                   // controlled by broker config. Partition numbering
	//                   // starts at 0.
	//
	// ============  =====  =======================================================
	// REQUEST_TYPE  VALUE  DEFINITION
	// ============  =====  =======================================================
	// PRODUCE         0    Send a group of messages to a topic and partition.
	// FETCH           1    Fetch a group of messages from a topic and partition.
	// MULTIFETCH      2    Multiple FETCH requests, chained together
	// MULTIPRODUCE    3    Multiple PRODUCE requests, chained together
	// OFFSETS         4    Find offsets before a certain time (this can be a bit
	//                      misleading, please read the details of this request).
	// ============  =====  =======================================================
	RequestHeader.prototype.serialize = function (stream) {
		return stream.write(this.header)
	}

	return RequestHeader
}
