# franz-kafka

A node client for [Kafka](http://incubator.apache.org/kafka/)

**This code is still in development and not ready for production use**

# Example

```js
var kafka = require('franz-kafka')


kafka.on('connect', function () {
	var foo = kafka.createTopic('foo')
	var bar = kafka.createTopic('bar')


	setInterval(
		function () {
			foo.publish("the time is: " + Date.now())
			bar.publish("a random number is: " + Math.random())
		},
		500
	)


	foo.on(
		'message',
		function (m) {
			console.log("foo offset: " + foo.offset)
			console.log(m.toString())
		}
	)

	bar.on(
		'message',
		function (m) {
			console.log("bar offset: " + bar.offset)
			console.log(m.toString())
		}
	)

	foo.consume(200)
	bar.consume(300)

	}
)
```

To test the example first get kafka running. Follow steps 1 and 2 of the (quick start guide)[http://incubator.apache.org/kafka/quickstart.html]

Then you can run `node example.js` to see messages getting published and consumed.

# TODO

* Connect to ZooKeeper to get broker info and store offsets
* Implement an "at-least-once" message strategy
* Implement partitions
* Implement multi-produce and multi-fetch
* Implement various configuration settings

# License

BSD