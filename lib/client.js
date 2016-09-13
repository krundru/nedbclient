var nssocket = require('nssocket');
var CALLBACK = function(){};

/**
 * Create a new database which maintains single socket connection to 
 * all its collections.
 * 
 * @param address valid formats are ip:port, port.
 * @param options
 * @returns a new database.
 */
exports.newDatabase = function(address, options) {  
  options = options || {};
  var _options = {};
  
  if (!address) {
    throw new Error('Address is missing');
  }
  
  if (typeof address === 'number' || /^\d+$/.test(address)) {
    _options.port = address;
    _options.host = '127.0.0.1';
  } else if (typeof address === 'number') {
    var tockets = address.split(':');
    _options.port = tockets[1];
    _options.host = tockets[0];
  }
  
   _options.reconnect = true;
   _options.type = 'tcp4';
   _options.retryInterval = options.retryInterval || 500;
  
   return new Connection(_options);
};

/**
 * creates a new collection.
 * 
 * @param address valid formats are ip:port, port.
 * @param options
 * @returns a new connection.
 */
function Connection(options) {
  this.options = options;
  this.socket = new nssocket.NsSocket(options);
  this.socket.connect(options.port, options.host);
}

/**
 * One place for writing & reading data.
 */
Connection.prototype.onCommand = function(request, callback) {
  try {
    this.socket.send('dbcall', request);
    this.socket.data('dbcall_cb_' + request.reqId, function(response) {
      callback.apply(this, response.data);
    });
  } catch (err) {
    callback(err, null);
  }
}

/**
 * Loads or creates new collection of a database.
 */
Connection.prototype.loadCollection = function(name) {
  if (!name) {
    throw new Error('missing collection name');
  }
  return new Collection(name, this);
};


/**
 * Returns a handler for new/existing collection.
 * 
 * @param address
 * @param options
 * @returns
 */
function Collection(name, connection) {
  this.connection = connection;
  this.name = name;
}

Collection.prototype.ensureIndex = function(data, callback) {
  execRemoteCall('ensureIndex', arguments, this);
};

Collection.prototype.removeIndex = function(data, callback) {
  execRemoteCall('removeIndex', arguments, this);
};

Collection.prototype.insert = function(data, callback) {
  execRemoteCall('insert', arguments, this);
};

Collection.prototype.update = function(data, callback) {
  execRemoteCall('update', arguments, this);
};

Collection.prototype.remove = function(data, callback) {
  execRemoteCall('remove', arguments, this);
};

Collection.prototype.find = function(data, callback) {
  execRemoteCall('find', arguments, this);
};

Collection.prototype.count = function(data, callback) {
  execRemoteCall('count', arguments, this);
};

function execRemoteCall(methodName, args, collection) {
  var list = Array.prototype.slice.call(args);
  // Resolve callback
  var callback = CALLBACK;
  if (typeof list[list.length -1] === 'function') {
    callback = list.pop();
  }

  var request = {
      reqId : generateUUID(),
      methodType : methodName,
      data: list,
      fileName: collection.name
  };
  
  collection.connection.onCommand(request, callback);  
}


/**
 * Generates UUID for request.
 */
function generateUUID() {
  var d = new Date().getTime();
  var uuid = 'xxxxxxxx.xxxx'.replace(/[xy]/g, function(c) {
      var r = (d + Math.random()*16)%16 | 0;
      d = Math.floor(d/16);
      return (c=='x' ? r : (r&0x3|0x8)).toString(16);
  });
  return uuid;
};
