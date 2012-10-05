var Kafka = require('./index')

var kafka = new Kafka({
	zookeeper: 'localhost:2181'
})

kafka.connect(function () {

	var foo = kafka.createTopic('foo')
	var bar = kafka.createTopic('bar')


	// setInterval(
	// 	function () {
	// 		foo.publish("the time is: " + Date.now())
	// 		bar.publish("a random number is: " + Math.random())
	// 	},
	// 	500
	// )


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
