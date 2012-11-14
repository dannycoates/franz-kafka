module.exports = function (
	RequestHeader,
	Response,
	OffsetsBody,
	int53) {

	function OffsetsRequest(topic, partition, time, maxCount) {
		this.topic = topic
		this.partition = partition
		this.time = time
		this.maxCount = maxCount
	}


	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// /                         REQUEST HEADER                        /
	// /                                                               /
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                              TIME                             |
	// |                                                               |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                     MAX_NUMBER (of OFFSETS)                   |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	//
	// TIME = int64 // Milliseconds since UNIX Epoch.
	//              // -1 = LATEST
	//              // -2 = EARLIEST
	// MAX_NUMBER = int32 // Return up to this many offsets
	OffsetsRequest.prototype.serialize =function (stream, cb) {
		var err = null
		var payload = new Buffer(12)
		int53.writeInt64BE(this.time, payload)
		payload.writeUInt32BE(this.maxCount, 8)
		var header = new RequestHeader(
			payload.length,
			RequestHeader.types.OFFSETS,
			this.topic.name,
			this.partition.id
		)
		try {
			header.serialize(stream)
			var written = stream.write(payload)
		}
		catch (e) {
			err = e
		}
		cb(err, written)
	}

	OffsetsRequest.prototype.response = function (cb) {
		return new Response(OffsetsBody, cb)
	}

	return OffsetsRequest
}
