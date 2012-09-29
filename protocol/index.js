var inherits = require('util').inherits
var zlib = require('zlib')
var snappy = require('snappy')
var crc32 = require('buffer-crc32')
var NullState = require('./nullstate')()
var State = require('./state')()
var Message = require('./message')(zlib, snappy, crc32)
var RequestHeader = require('./request-header')()
var ResponseHeader = require('./response-header')(inherits, State)
var FetchBody = require('./fetch-body')(inherits, State, Message)
var OffsetsBody = require('./offsets-body')(inherits, State)
var Response = require('./response')(ResponseHeader, FetchBody, OffsetsBody)
var Receiver = require('./receiver')(NullState, Response)
var FetchRequest = require('./fetch-request')(RequestHeader)
var OffsetsRequest = require('./offsets-request')(RequestHeader)
var ProduceRequest = require('./produce-request')(RequestHeader, Message)

module.exports = {
	Receiver: Receiver,
	FetchRequest: FetchRequest,
	ProduceRequest: ProduceRequest,
	Message: Message
}
