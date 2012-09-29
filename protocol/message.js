module.exports = function (zlib, snappy, crc32) {

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
			var self = this
			function uncompressed(err, data) {
				if (err) {
					return cb(err)
				}
				self.payload = data
				self.checksum = crc32.unsigned(data)
				self.compression = Message.compression.NONE
				cb(null, data)
			}
			if (this.compression === Message.compression.GZIP) {
				zlib.gunzip(this.payload, uncompressed)
			}
			else if (this.compression === Message.compression.SNAPPY) {
				snappy.uncompress(this.payload, uncompressed)
			}
			else {
				cb(new Error("Unknown compression " + this.compression))
			}
		}
		else {
			cb(null, this.payload)
		}
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

	Message.prototype.setData = function (buffer, compression, cb) {
		var self = this
		if (compression) {
			this.magic = 1
			this.compression = compression
			compress(buffer, compression,
				function (err, compressed) {
					if (err) {
						return cb(err)
					}
					self.payload = compressed
					self.checksum = crc32.unsigned(compressed)
					return cb(null, self)
				}
			)
		}
		else {
			this.magic = 1
			this.compression = 0
			this.payload = buffer
			this.checksum = crc32.unsigned(buffer)
			cb(null, this)
		}
	}

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
