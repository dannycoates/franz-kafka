module.exports = function (
	zlib,
	snappy,
	crc32) {

	function Message() {
		this.magic = 0
		this.compression = 0
		this.checksum = 0
		this.payload = null
	}

	Message.compression = {
		NONE: 0,
		GZIP: 1,
		SNAPPY: 2
	}

	Message.create = function (data) {
		if(!Buffer.isBuffer(data)) {
			if (data instanceof Message) {
				return data
			}
			data = new Buffer(data)
		}

		var m = new Message()
		m.magic = 1
		m.compression = Message.compression.NONE
		m.payload = data
		m.checksum = crc32.unsigned(data)
		return m
	}

	Message.prototype.length = function () {
		return this.payload.length + 6
	}

	Message.prototype.toBuffer = function () {
		var buffer = new Buffer(this.payload.length + 10)
		buffer.writeUInt32BE(this.payload.length + 6, 0)
		buffer.writeUInt8(this.magic, 4)
		buffer.writeUInt8(this.compression, 5)
		buffer.writeUInt32BE(this.checksum, 6)
		this.payload.copy(buffer, 10)
		return buffer
	}

	Message.prototype.getData = function (cb) {
		if (this.magic && this.compression) {
			if (this.compression === Message.compression.GZIP) {
				zlib.gunzip(this.payload, afterUncompress.bind(this, cb))
			}
			else if (this.compression === Message.compression.SNAPPY) {
				snappy.uncompress(this.payload, afterUncompress.bind(this, cb))
			}
			else {
				cb(new Error("Unknown compression " + this.compression))
			}
		}
		else {
			cb(null, this.payload)
		}
	}

	function afterUncompress(cb, err, data) {
		if (err) {
			return cb(err)
		}
		this.payload = data
		this.checksum = crc32.unsigned(data)
		this.compression = Message.compression.NONE
		cb(null, data)
	}

	function compress(buffer, method, cb) {
		switch (method) {
			case Message.compression.GZIP:
				zlib.gzip(buffer, cb)
				break;
			case Message.compression.SNAPPY:
				snappy.compress(buffer, cb)
				break;
			default:
				cb(null, buffer)
		}
	}

	function afterCompress(cb, err, compressed) {
		if (err) {
			return cb(err)
		}
		this.payload = compressed
		this.checksum = crc32.unsigned(compressed)
		return cb(null, this)
	}

	Message.prototype.setData = function (buffer, compression, cb) {
		if (compression) {
			this.magic = 1
			this.compression = compression
			compress(buffer, compression, afterCompress.bind(this, cb))
		}
		else {
			this.magic = 1
			this.compression = 0
			this.payload = buffer
			this.checksum = crc32.unsigned(buffer)
			cb(null, this)
		}
	}

	Message.prototype.unpack = function (cb) {
		var payloads = []
		if (this.compression) {
			this.getData(
				function (err, data) {
					var offset = 0
					while (offset < data.length) {
						var len = data.readUInt32BE(offset)
						payloads.push(
							Message.parse(
								data.slice(offset, offset + len + 4)
							)
							.payload
						)
						offset += (len + 4)
					}
					cb(payloads)
				}
			)
		}
		else {
			cb([this.payload])
		}
	}

	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                             LENGTH                            |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |     MAGIC       |  COMPRESSION  |           CHECKSUM          |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |      CHECKSUM (cont.)           |           PAYLOAD           /
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                             /
	// /                         PAYLOAD (cont.)                       /
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

	// LENGTH = int32 // Length in bytes of entire message (excluding this field)
	// MAGIC = int8 // 0 = COMPRESSION attribute byte does not exist (v0.6 and below)
	//              // 1 = COMPRESSION attribute byte exists (v0.7 and above)
	// COMPRESSION = int8 // 0 = none; 1 = gzip; 2 = snappy;
	//                    // Only exists at all if MAGIC == 1
	// CHECKSUM = int32  // CRC32 checksum of the PAYLOAD
	// PAYLOAD = Bytes[] // Message content
	Message.parse = function (buffer) {
		var m = new Message()
		var payload = new Buffer(buffer.length - 10)
		buffer.copy(payload, 0, 10)

		var i = 4
		m.magic = buffer.readUInt8(i++)
		if (m.magic) {
			m.compression = buffer.readUInt8(i++)
		}
		m.checksum = buffer.readUInt32BE(i)
		m.payload = payload // TODO maybe slice to save a copy
		if (crc32.unsigned(payload) !== m.checksum) {
			console.error("Mismatched checksum")
		}
		return m
	}

	return Message
}
