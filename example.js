var Kafka = require('./index')

var kafka = new Kafka({
	//zookeeper: 'localhost:2181',
	brokers: [{
		id: 0,
		host: 'localhost',
		port: 9092,
		topics: {
			foo: 2,
			bar: 2
		}
	}],
	compression: 'gzip',
	queueTime: 2000,
	batchSize: 200
})

kafka.connect(function () {

	//var foo = kafka.topic('foo')
	var bar = kafka.consume('bar', 200, ['0-0', '0-1'])

	bar.on(
		'message',
		function (m) {
			console.log(m.toString())
		}
	)

	setInterval(
		function () {
			bar.publish("the time is: " + Date.now())
			//foo.publish("a random number is: " + Math.random())
		},
		100
	)

	}
)
