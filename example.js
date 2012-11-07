var Kafka = require('./index')
var fs = require('fs')

var file = fs.createWriteStream('./test.txt')

var kafka = new Kafka({
	zookeeper: 'localhost:2181',
	// brokers: [{
	// 	id: 0,
	// 	host: 'localhost',
	// 	port: 9092,
	// 	topics: {
	// 		bar: 1
	// 	}
	// }],
	compression: 'gzip',
	queueTime: 2000,
	batchSize: 200,
	maxFetchSize: 20480,
	logger: console
})

file.once('open', function () {

	kafka.connect(function () {

		var bar = kafka.topic('bar')

		bar.pipe(file)

		setInterval(
			function () {
				bar.write("the time is: " + Date.now())
			},
			10
		)

		bar.resume()

		}
	)
})
