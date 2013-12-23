var stream = require('stream')
  , util = require('util')
  ;

function values (obj) {
  return Object.keys(obj).map(function (key) {return obj[key]})
}

function FuncStream ()  {
  stream.Transform.apply(this, arguments)
  this._readableState.objectMode = true;
  this._writableState.objectMode = true;
}
util.inherits(FuncStream, stream.Transform)

function mutate (stream, fn) {
  var ret = funcstream(function (data, enc, cb) {
    ret.push(fn(data))
    cb()
  })
  stream.pipe(ret)
  return ret
}
function filter (stream, fn) {
  var ret = funcstream(function (data, enc, cb) {
    if (fn(data)) ret.push(data)
    cb()
  })
  stream.pipe(ret)
  return ret
}
function asyncMutate (stream, fn) {
  var ret = funcstream(function (data, enc, cb) {
    fn(data, function (e, d) {
      if (e) return cb(e)
      ret.push(d)
      cb()
    })
  })
  stream.pipe(ret)
  return ret
}
function asyncFilter (stream, fn) {
  var ret = funcstream(function (data, enc, cb) {
    fn(data, function (e, b) {
      if (e) return cb(e)
      if (b) ret.push(data)
      cb()
    })
  })
  stream.pipe(ret)
  return ret
}

FuncStream.prototype.filter = function (fn) {
  return filter(this, fn)
}
FuncStream.prototype.asyncFilter = function (fn) {
  return asyncFilter(this, fn)
}
FuncStream.prototype.map = function (fn) {
  return mutate(this, fn)
}
FuncStream.prototype.asyncMap = function (fn) {
  return asyncMutate(this, fn)
}
FuncStream.prototype.reduce = function (fn, pre, cb) {
  if (typeof pre === 'function') {
    cb = pre
    pre = undefined
  }
  var ret = mutate(this, function (data) {
    pre = fn(pre, data)
    return pre
  })
  if (cb) {
    ret.on('error', cb)
    ret.on('end', function () {
      cb(null, pre)
    })
    ret.on('data', function () {}) // hack
  }
  return ret
}
FuncStream.prototype.asyncReduce = function (fn, pre) {
  var ret = asyncMutate(this, function (data, cb) {
    fn(pre, data, function (e, r) {
      if (e) return cb(e)
      pre = r
      cb(null, r)
    })
  })
  return ret
}
FuncStream.prototype.pluck = function (prop) {
  var fn
  if (typeof prop === 'string') fn = function (data) { return data[prop] }
  else fn = prop
  return mutate(this, fn)
}
FuncStream.prototype.compact = function (cb) {
  var buff = []
    , ret = mutate(this, function (data) { buff.push(data); return buff})
    ;
  if (cb) {
    cb = once(cb)
    ret.on('error', cb)
    ret.on('end', function () {
      cb(null, buff)
    })
    ret.on('data', function () {}) // hack
  }
  return ret
}
FuncStream.prototype.sum = function (cb) {
  return this.reduce(function (x, y) { return x + y }, 0, cb)
}
FuncStream.prototype.uniq = function (map, compare) {
  if (!map) map = function (x) {return x}
  if (!compare) compare = function (x,y) {return x === y}
  var buff = []
    , ret = filter(this, function (data) {
      data = map(data)
      for (var i=0;i<buff.length;i++) {
        if (compare(data, buff[i])) return false
      }
      buff.push(data)
      return true
    })
    ;
  return ret
}
FuncStream.prototype.group = function (prop) {
  var ret = funcstream()
    , groups = {}
    , fn
    ;
  if (typeof prop === 'string') fn = function (data) {return data[prop]}
  else fn = prop
  var self = this
  this.on('data', function (data) {
    var key = fn(data)
    if (!groups[key]) {
      groups[key] = funcstream()
      groups[key].key = key
      ret.write(groups[key])
    }
    var written = groups[key].write(data)
    // if (!written) {
    //   self.pause()
    //   groups[key].on('drain', function () {self.resume()})
    // }
  })
  this.on('end', function () {
    values(groups).forEach(function (stream) {
      stream.end()
    })
  })
  return ret
}
FuncStream.prototype.each = function (fn) {
  this.on('data', fn)
  return this
}
FuncStream.prototype.count = function (cb) {
  return this.map(function () {return 1}).sum(cb)
}

function funcstream (transform) {
  var ret = new FuncStream()
  ret._transform = transform || function (data, enc, cb) {ret.push(data); cb()}
  return ret
}

module.exports = funcstream
