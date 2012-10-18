var Kafka = require('./index')

var kafka = new Kafka({
	zookeeper: 'localhost:2181'
})

kafka.connect(function () {

	var foo = kafka.topic('foo')
	var bar = kafka.consume('bar', 200)

	bar.on(
		'message',
		function (m) {
			console.log("bar offset: " + bar.offset)
			console.log(m.toString())
		}
	)

	setInterval(
		function () {
			foo.publish("the time is: " + Date.now())
			bar.publish("a random number is: " + Math.random())
		},
		500
	)

	}
)
