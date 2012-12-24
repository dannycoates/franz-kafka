module.exports = function () {
	function RingList() {
		this.items = []
		this.index = 0
	}

	RingList.prototype.add = function (item) {
		if (this.items.indexOf(item) < 0) {
			this.items.push(item)
		}
	}

	RingList.prototype.remove = function (item) {
		var i = this.items.indexOf(item)
		if (i >= 0) {
			this.items.splice(i, 1)
		}
	}

	RingList.prototype.next = function (filter) {
		if (filter) {
			var item = null
			for (var i = 0; i < this.items.length; i++) {
				item = this.next()
				if (filter(item)) {
					return item
				}
			}
			return null
		}
		this.index = (this.index + 1) % this.items.length
		return this.current()
	}

	RingList.prototype.current = function () {
		return this.items[this.index]
	}

	return RingList
}
