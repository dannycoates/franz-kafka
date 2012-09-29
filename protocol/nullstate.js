module.exports = function () {
	function NullState() {}
	NullState.prototype.read = function () { return false }
	NullState.prototype.complete = function () { return true }
	return NullState
}