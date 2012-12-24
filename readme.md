# franz-kafka

A node client for [Kafka](http://incubator.apache.org/kafka/)

# Example

```js
var Kafka = require('franz-kafka')

var kafka = new Kafka({
	zookeeper: ['localhost:2181'],
	compression: 'gzip',
	queueTime: 2000,
	batchSize: 200,
	logger: console
})

kafka.connect(function () {

	// topics are Streams
	var foo = kafka.topic('foo')
	var bar = kafka.topic('bar')

	// consume with a pipe
	foo.pipe(process.stdout)

	// or with the 'data' event
	foo.on('data', function (data) { console.log(data) })

	// produce with a pipe
	process.stdin.pipe(bar)

	// or just write to it
	bar.write('this is a message')

	// resume your consumer to get it started
	foo.resume()

	// don't forget to handle errors
	foo.on('error', function (err) { console.error("STAY CALM") })

	}
)
```

To test the example first get kafka running. Follow steps 1 and 2 of the [quick start guide](http://incubator.apache.org/kafka/quickstart.html)

Then you can run `node example.js` to see messages getting produced and consumed.

---

# API

## Kafka

### new

```js
var Kafka = require('franz-kafka')

var kafka = new Kafka({
	brokers: [{              // an array of broker connection info
		id: 0,               // the server's broker id
		host: 'localhost',
		port: 9092
	}],

	// producer defaults
	compression: 'none',     // default compression for producing
	maxMessageSize: 1000000, // limits the size of a produced message
	queueTime: 5000,         // milliseconds to buffer batches of messages before producing
	batchSize: 200,          // number of messages to bundle before producing

	// consumer defaults
	groupId: 'franz-kafka',  // the consumer group name this instance is part of
	minFetchDelay: 0,        // minimum milliseconds to wait between fetches
	maxFetchDelay: 10000,    // maximum milliseconds to wait between fetches
	maxFetchSize: 300*1024,  // limits the size of a fetched message

	logger: null             // a logger that implements global.console (for debugging)
})
```
###### brokers

An array of connection info of all the brokers this client can communicate with

###### compression

The compression used when producing to kafka. May be, 'none', 'gzip', or 'snappy'

###### maxMessageSize

The largest size of a message produced to kafka. If a message exceeds this size,
the Topic will emit an 'error'. Note that `batchSize` affects the size of messages
because batches of messages are bundled as individual messages.

###### queueTime

The time to buffer messages for bundling before producing to kafka. This option
is combined with `batchSize`. Whichever comes first will trigger a produce.

###### batchSize

The number of messages to bundle before producing to kafka. This option
is combined with `queueTime`. Whichever comes first will trigger a produce.

###### minFetchDelay

The minimum time to wait between fetch requests to kafka. When a fetch returns
zero messages the client will begin exponential backoff between requests up to
`maxFetchDelay` until messages are available.

###### maxFetchDelay

The maximum time to wait between fetch requests to kafka after exponential
backoff has begun.

###### maxFetchSize

The maximum size of a fetched message. If a fetched message is larger than this
size the Topic will emit an 'error' event.


### kafka.connect([onconnect])

Connects to the Kafka cluster and runs the callback once connected.

```js
kafka.connect(function () {
	console.log('connected')
	//...
})
```

### kafka.topic(name, [options])

Get a Topic for consuming or producing. The first argument is the topic name and
the second are the topic options.

```js
var foo = kafka.topic('foo', {
	// default options
	minFetchDelay: 0,      // defaults to the kafka.minFetchDelay
	maxFetchDelay: 10000,  // defaults to the kafka.maxFetchDelay
	maxFetchSize: 1000000, // defaults to the kafka.maxFetchSize
	compression: 'none',   // defaults to the kafka.compression
	batchSize: 200,        // defaults to the kafka.batchSize
	queueTime: 5000,       // defaults to the kafka.queueTime
	partitions: {
		consume: ['0-0:0'],  // array of strings with the form 'brokerId-partitionId:startOffset'
		produce: ['0:1']     // array of strings with the form 'brokerId:partitionCount'
	}
})
```

##### partitions

This structure describes which brokers and partitions the client will connect to
for producing and consuming.

###### consume

An array of partitions to consume and what offset to begin consuming from in the
form of 'brokerId-partitionId:startOffset'. For example broker 2 partition 3
offset 5 is '2-3:5'

###### produce

An array of brokers to produce to with the count of partitions in the form of
'brokerId:partitionCount'. For example broker 3 with 8 partitions is '3:8'

### events

###### connect

Fires when the client is connected to a broker.


## Topic

A topic is a [Stream](http://nodejs.org/api/stream.html) that may be Readable for
consuming and Writable for producing. Retrieve a topic from the kafka instance.

```js
var topic = kafka.topic('a topic')
```

### topic.pause()

Pause the consumer stream

### topic.resume()

Resume the consumer stream

### topic.destroy()

Destroy the consumer stream

### topic.setEncoding([encoding])

Sets the encoding of the data emitted by the `data` event.

* `encoding` is one of:
	'utf8', 'utf16le', 'ucs2', 'ascii', 'hex', or undefined for a raw Buffer

### topic.write(data, [encoding])

Write a message to the topic. Returns false if the message buffer is full.

* `data` is a string or Buffer
* `encoding` is optional when `data` is a string. Default is 'utf8'

### topic.end(data, [encoding])

Same as `write`

### topic.pipe(destination, [options])

Pipe the stream of messages to the `destination` Writable Stream.
See [Stream.pipe](http://nodejs.org/api/stream.html#stream_stream_pipe_destination_options)

### events

###### data

Fires for each message. Data is a Buffer by default or a string if `setEncoding`
was called.

```js
topic.on('data', function (data) { console.log('message data: %s', data) })
```

###### drain

Fires when the producer stream can handle more messages

```js
topic.on('drain', function () { console.log('%s is ready to write', topic.name )})
```

###### error

Fires when there is a produce or consume error. An Error object is emitted.

```js
topic.on('error', function (error) { console.error(error.message)})
```

###### offset

Fires when a new offset is fetched. 'data' events emitted between 'offset' events
all belong to the same offset in Kafka. The partition name and offset is emitted.

```js
topic.on('offset', function(partition, offset) {
	console.log("topic: %s partition: %s offset: %d", topic.name, partition, offset)
})
```


---

## ZooKeeper

ZooKeeper support is in development. Producer support is functional. Consumer support
does not yet include partition balancing.

To produce using ZooKeeper use the `zookeeper` option in the Kafka constructor.

```js
var kafka = new Kafka({
	zookeeper: ['localhost:2181'] // array of 'host:port' zookeeper nodes
})

kafka.connect(function () {
	console.log('connected via zookeeper')
})
```

# License

MIT
