var Kafka = require('./index')

var kafka = new Kafka({
	zookeeper: 'localhost:2181',
	// brokers: [{
	// 	id: 0,
	// 	host: 'localhost',
	// 	port: 9092,
	// 	topics: {
	// 		//foo: 2,
	// 		bar: 1
	// 	}
	// }],
	compression: 'gzip',
	queueTime: 2000,
	batchSize: 200,
	logger: console
})

kafka.connect(function () {

	//var foo = kafka.topic('foo')
	var bar = kafka.topic('bar')

	bar.on(
		'data',
		function (m) {
			//console.log(m.toString())
		}
	)

	setInterval(
		function () {
			bar.write("the time is: " + Date.now())
			//foo.publish("a random number is: " + Math.random())
		},
		100
	)

	bar.resume()

	}
)
