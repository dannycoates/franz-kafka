var Kafka = require('./index')
var fs = require('fs')

var file = fs.createWriteStream('./test.txt')

var kafka = new Kafka({
	//zookeeper: 'localhost:2181',
	brokers: [{
		id: 0,
		host: 'localhost',
		port: 9092
	}],
	compression: 'gzip',
	queueTime: 2000,
	batchSize: 200,
	//maxFetchSize: 128,
	//maxMessageSize: 1,
	logger: console
})
var i = 0

file.once('open', function () {
	kafka.connect(function () {

		var baz = kafka.topic('bazzz', {
			partitions: {
				consume: ['0-0'],
				produce: ['0:1']
			}
		})

		baz.pipe(file)
		baz.resume()

		baz.on('error', function (err) {
			switch(err.name) {
				case 'Produce Error':
					this.maxMessageSize = err.length
					break;
				case 'Fetch Error':
					this.maxFetchSize = err.messageLength * 2
					console.error('new size', this.maxFetchSize)
					break;
			}
			this.resume()
		})

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
