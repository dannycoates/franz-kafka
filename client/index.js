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
	var FetchBody = require('./fetch-body')(inherits, State, Message)
	var OffsetsBody = require('./offsets-body')(inherits, State, int53)
	var Response = require('./response')(State, ResponseHeader)
	var Receiver = require('./receiver')(inherits, EventEmitter, State)
	var FetchRequest = require('./fetch-request')(RequestHeader, Response, FetchBody, int53)
	var OffsetsRequest = require('./offsets-request')(RequestHeader, Response, OffsetsBody, int53)
	var ProduceRequest = require('./produce-request')(RequestHeader, Message, State)
	var Client = require('./client')(
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
