var net = require('net')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var zlib = require('zlib')
var ReadableStream = require('readable-stream')
try {
	var snappy = require('snappy')
}
catch (e) {
	function snappyError() { throw new Error('snappy could not be loaded')}
	var snappy = {
		compress: snappyError,
		uncompress: snappyError
	}
}
var int53 = require('int53')
var crc32 = require('buffer-crc32')

module.exports = function (logger) {
	var State = require('./state')(inherits)
	var Message = require('./message')(zlib, snappy, crc32)
	var RequestHeader = require('./request-header')()
	var ResponseHeader = require('./response-header')(inherits, State)
	var FetchResponse = require('./fetch-response')(inherits, State, Message)
	var OffsetsResponse = require('./offsets-response')(inherits, State, int53)
	var Response = require('./response')(logger, State, ResponseHeader)
	var Receiver = require('./receiver')(logger, inherits, EventEmitter, State)
	var FetchRequest = require('./fetch-request')(RequestHeader, Response, FetchResponse, int53)
	var OffsetsRequest = require('./offsets-request')(RequestHeader, Response, OffsetsResponse, int53)
	var ProduceRequest = require('./produce-request')(inherits, RequestHeader, Message, State)
	var Client = require('./client')(
		logger,
		net,
		inherits,
		EventEmitter,
		ReadableStream,
		Message,
		Receiver,
		FetchRequest,
		ProduceRequest,
		OffsetsRequest
	)
	return Client
}
