var nssocket = require('nssocket');
var CALLBACK = function(){};

/**
 *  
 * @param address
 * @param options
 * @returns
 */
var Database = exports.Database = function(options) {
  if (!options || !options.port || !options.fileName) {
    throw new Error('missing required either port or fileName options');
  }
  
  this.options = {
      reconnect: true,
      type: 'tcp4',
      host: options.host || '127.0.0.1',
      fileName: options.fileName,
      port: options.port,
      retryInterval: options.retryInterval || 500,
      counter: 1000      
  };

  this.socket = new nssocket.NsSocket(this.options);
  this.socket.connect(this.options.port, this.options.host);
}


Database.prototype.ensureIndex = function(data, callback) {
  execRemoteCall('ensureIndex', arguments, this);
};

Database.prototype.removeIndex = function(data, callback) {
  execRemoteCall('removeIndex', arguments, this);
};

Database.prototype.insert = function(data, callback) {
  execRemoteCall('insert', arguments, this);
};

Database.prototype.update = function(data, callback) {
  execRemoteCall('update', arguments, this);
};

Database.prototype.remove = function(data, callback) {
  execRemoteCall('remove', arguments, this);
};

Database.prototype.find = function(data, callback) {
  execRemoteCall('find', arguments, this);
};

Database.prototype.count = function(data, callback) {
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
    console.log(response);
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
