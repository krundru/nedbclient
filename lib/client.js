var nssocket = require('nssocket');
var CALLBACK = function(){};

/**
 * Create a collection factory which maintains single socket connection to 
 * all its collections.
 * 
 * @param address valid formats are ip:port, port.
 * @param options
 * @returns a new collection factory.
 */
exports.newCollectionFactory = function(address, options) {  
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
  
   return new CollectionFactory(_options);
};


/**
 * Collection factory.
 * 
 * @param address valid formats are ip:port, port.
 * @param options
 * @returns a new connection factory.
 */
function CollectionFactory(options) {
  var socket = new nssocket.NsSocket(options);
  socket.connect(options.port, options.host);

  Collection.prototype.socket = socket;
}

/**
 * Loads or creates new collection from collection factory. 
 */
CollectionFactory.prototype.loadCollection = function(name) {
  if (!name) {
    throw new Error('missing collection name');
  }
  var collection = new Collection(name);
  // its loads collections in memory.
  collection.count({});
  return collection;
};

/**
 * Returns a handler for new/existing collection.
 * 
 * @param address
 * @param options
 * @returns
 */
function Collection(name) {
  this.options = {
    fileName: name,
    counter: 1000
  };
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

function execRemoteCall(methodName, args, client) {
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
      fileName: client.options.fileName
  };
  
  client.socket.send('dbcall', request);
  client.socket.data('dbcall_cb_' + request.reqId, function(response) {
    callback.apply(this, response.data);
  });
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
