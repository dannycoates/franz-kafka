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
	// 		baz: 1
	// 	}
	// }],
	compression: 'gzip',
	queueTime: 2000,
	batchSize: 200,
	//maxFetchSize: 20480,
	logger: console
})
var i = 0

file.once('open', function () {
	kafka.connect(function () {

		var baz = kafka.topic('bazzz')

		baz.pipe(file)
		baz.resume()

		setInterval(
			function () {
				baz.write('i is ' + i + '\n')
				i++
				//baz.write("the time is: " + Date.now())
			},
			10
		)

		}
	)
})
