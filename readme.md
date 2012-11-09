# franz-kafka

A node client for [Kafka](http://incubator.apache.org/kafka/)

**This code is still in development and not ready for production use**

# Example

```js
var Kafka = require('franz-kafka')

var kafka = new Kafka({
	zookeeper: 'localhost:2181',
	compression: 'gzip',
	queueTime: 2000,
	batchSize: 200,
	logger: console
})

kafka.on('connect', function () {

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

Then you can run `node example.js` to see messages getting published and consumed.

# License

BSD
