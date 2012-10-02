module.exports = function (
	inherits,
	EventEmitter,
	Client,
	Topic) {

	function Kafka() {
		var self = this
		this.client = new Client({
			port: 9092
		})
		this.client.on(
			'connect',
			function () {
				self.emit('connect')
			}
		)
		this.topics = {}
	}
	inherits(Kafka, EventEmitter)

	Kafka.prototype.createTopic = function (name) {
		var t = new Topic(name, this)
		this.topics[name] = t
		return t
	}

	Kafka.prototype.consume = function (topic, interval) {
		//TODO: a better interval method
		var self = this
		clearInterval(topic.interval)
		setInterval(
			function () {
				self.client.fetch(topic)
			},
			interval
		)
	}

	Kafka.prototype.publish = function (topic, messages) {
		this.client.produce(topic.name, messages)
	}

	return new Kafka()
}