var nssocket = require('nssocket');
var CALLBACK = function(){};

/**
 * Creates a new collection.
 * 
 * @param address valid formats are ip:port, port.
 * @param name name of the collection.
 * 
 * @returns a new collection.
 */
exports.loadCollection = function(address, name) {    
  var _options = {};
  
  if (!address  || !name) {
    throw new Error('Address or name is missing');
  }
  
  if (typeof address === 'number' || /^\d+$/.test(address)) {
    _options.port = address;
    _options.host = '127.0.0.1';
  } else if (typeof address === 'string') {
    var tockets = address.split(':');
    _options.port = tockets[1];
    _options.host = tockets[0];
  }
  
  _options.name = name;

  return new Collection(_options);
};


/**
 * Returns a handler for new/existing collection.
 * 
 * @param address
 * @param options
 * @returns
 */
function Collection(options) {
  this.options = options;
}

Collection.prototype.ensureIndex = function(/* arguments */) {
  process.nextTick(execMethod, 'ensureIndex', arguments, this.options);
};

Collection.prototype.removeIndex = function(/* arguments */) {
  process.nextTick(execMethod, 'removeIndex', arguments, this.options);
};

Collection.prototype.insert = function(/* arguments */) {
  process.nextTick(execMethod, 'insert', arguments, this.options);
};

Collection.prototype.update = function(/* arguments */) {
  process.nextTick(execMethod, 'update', arguments, this.options);
};

Collection.prototype.remove = function(/* arguments */) {
  process.nextTick(execMethod, 'remove', arguments, this.options);
};

Collection.prototype.find = function(/* arguments */) {
  var list = Array.prototype.slice.call(arguments);
  if (typeof list[list.length -1] === 'function') {
    return process.nextTick(execMethod, 'find', arguments, this.options);
  }
  return new Find(list, this.options);
};

function Find(args, options) {
  this.query =  {args:args, name: 'find'};
  this.colConfig = options;
}

Find.prototype.sort = function(sort) {
  this.query.sort = [sort];
  return this;
}

Find.prototype.skip = function(skip) {
  this.query.skip = [skip];
  return this;
}

Find.prototype.limit = function(limit) {
  this.query.limit = [limit];
  return this;
}

Find.prototype.projection = function(projection) {
  this.query.projection = [projection];
  return this;
}

Find.prototype.exec = function(callback) {
  this.query.call = 'exec';
  var args = {0: this.query, 1: callback, length:2};
  process.nextTick(execMethod, 'findplus', args, this.colConfig);
}

Collection.prototype.count = function(/* arguments */) {
  process.nextTick(execMethod, 'count', arguments, this.options);
};

/**
 * 
 * @param methodName
 * @param args
 * @param options
 * @returns
 */
function execMethod(methodName, args, options) {
  var list = Array.prototype.slice.call(args);
  // Resolve callback.
  var callback = CALLBACK;
  if (typeof list[list.length -1] === 'function') {
    callback = list.pop();
  }
  // Prepare request.
  var request = {
    methodType : methodName,
    data: list,
    fileName: options.name
  };
  var socket = new nssocket.NsSocket(options);
  socket.once('error', callback);
  socket.connect(options.port, options.host, function() {
    socket.send('dbcall', request);    
    socket.data('dbcall_cb', function(response) {
      socket.end();
      callback.apply({}, response.data);      
    });
  });
}