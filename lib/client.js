var nssocket = require('nssocket');

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
  
};

Database.prototype.removeIndex = function(data, callback) {
  
};

Database.prototype.insert = function(data, callback) {
  var request = {
      methodType : 'insert',
      data: [data],
      fileName: this.options.fileName
  }  
  remoteCall(this.socket, request, callback);
};

Database.prototype.update = function(data, callback) {
  
};

Database.prototype.remove = function(data, callback) {
  
};

Database.prototype.find = function(data, callback) {
  
};

Database.prototype.count = function(data, callback) {
  
};

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

/**
 * Call remote method.
 */
function remoteCall(client, request, callback) {
  request.reqId = generateUUID();
  client.send('dbcall', request);
  client.data('dbcall_cb_' + request.reqId, function(response) {
    callback(response.error, response.data);
  });
}