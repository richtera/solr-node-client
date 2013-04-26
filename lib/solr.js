/*!
 * solr client
 * Copyright(c) 2011-2012 HipSnip Limited
 * Author Rémy Loubradou <remyloubradou@gmail.com>
 * MIT Licensed
 */
 
/**
 * Load dependencies
 */

var http = require('http'),
   Query = require('./query'),
   querystring = require('querystring'),
   format = require('./utils/format'),
   SolrError = require('./error/solr-error'),
   JSONStream = require('JSONStream'),
   duplexer = require('duplexer'),
   request = require('request');

/**
 * Expose `createClient()`.
 */
 
exports.createClient = createClient;

/**
 * Create an instance of `Client`
 *
 * @param {String|Object} [host='127.0.0.1'] - IP address or host address of the Solr server
 * @param {Number|String} [port='8983'] - port of the Solr server
 * @param {String} [core=''] - name of the Solr core requested
 * @param {String} [path='/solr'] - root path of all requests
 *
 * @return {Client}
 * @api public
 */

function createClient(host, port, core, path){
  var options = (typeof host === 'object')? host : {
      host : host,
      port : port,
      core : core,
      path : path
   };
  return new Client(options);
}

/**
 * Create a new `Client`
 * @constructor
 *
 * @param {Object} options - set of options used to request the Solr server
 * @param {String} options.host - IP address or host address of the Solr server
 * @param {Number|String} options.port - port of the Solr server
 * @param {String} options.core - name of the Solr core requested
 * @param {String} options.path - root path of all requests
 * 
 * @return {Client}
 * @api private
 */
 
var Client = function(options){
   this.options = {
      hosts: options.hosts || ([(options.host || '127.0.0.1') + ":" + (options.port || '8983')]),
      core : options.core || '',
      path : options.path || '/solr',
      timeout: options.timeout
   };
   this.dead = [];
   this.autoCommit = false;
}

Client.prototype.getOptions = function (options) {
  var out = {};
  var self = this;
  Object.keys(this.options).forEach(function (e) {
    out[e] = self.options[e];
  });
  if (options) {
    Object.keys(options).forEach(function (e) {
      out[e] = options[e];
    });
  }
  return out;
}

Client.prototype.getHost = function() {
  var len = this.options.hosts.length,
      rnd = Math.floor(Math.random() * len);
  if (rnd >= len)
    rnd = len - 1;
  return this.options.hosts[rnd];
}

/**
 * Create credential using the basic access authentication method
 *
 * @param {String} username
 * @param {String} password
 *
 * @return {Client}
 * @api public
 */

Client.prototype.basicAuth = function(username,password){
   var self = this;
   this.options.authorization = 'Basic ' + new Buffer(username + ':' + password).toString('base64');
   return self;
}

/**
 * Remove authorization header
 *
 * @return {Client}
 * @api public
 */

Client.prototype.unauth = function(){
   var self = this;
   delete this.options.authorization;
   return self;
}
 
/**
 * Add a document or a list of documents
 * 
 * @param {Object|Array} doc - document or list of documents to add into the Solr database
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 *
 * @return {Client}
 * @api public 
 */ 

Client.prototype.add = function(docs,options,callback){
   var self = this;
   if (!callback) {
    callback = options;
    options = {};
   }
   if (!options.extract) {
     docs = format.dateISOify(docs); // format `Date` object into string understable for Solr as a date.
     docs = Array.isArray(docs) ? docs : [docs];
   }
   this.update(docs,options,callback);
   return self;
}

/**
 * Add the remote resource located at the given path `options.path` into the Solr database.
 * 
 * @param {Object} options -
 * @param {String} options.path - path of the file. HTTP URL, the full path or a path relative to the CWD of the running solr server must be used.
 * @param {String} [options.format='xml'] - format of the resource. XML, CSV or JSON formats must be used.
 * @param {String} [options.contentType='text/plain;charset=utf-8'] - content type of the resource
 * @param {Object} [options.parameters] - set of extras parameters pass along in the query. 
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 *
 * @return {Client}
 * @api public
 */

Client.prototype.addRemoteResource = function(options,callback){
   var self = this;
   options = this.getOptions(options);
   options.parameters = options.parameters || {};
   options.format = (options.format === 'xml' ? '' : options.format || ''); // reason: the default route of the XmlUpdateRequestHandle is /update and not /update/xml.
   options.parameters.commit = (options.parameters.commit === undefined ? this.autoCommit : options.parameters.commit);
   options.parameters['stream.contentType'] = options.contentType || 'text/plain;charset=utf-8';
   if(options.path.match(/^https?:\/\//)){
      options.parameters['stream.url'] = options.path;
   }else{
      options.parameters['stream.file'] = options.path;
   }
   var path = 'update/' + options.format.toLowerCase()  + '?' + querystring.stringify(options.parameters) + '&wt=json';
   options.fullPath = [options.path, options.core, path]
                              .filter(function(element){
                                 if(element) return true;
                                 return false;    
                              })
                              .join('/');
   this.queryRequest(options,callback);
   return self;
}

/**
 * Create a writable/readable `Stream` to add documents into the Solr database
 *
 * return {Stream}
 * @api public
 */

Client.prototype.createAddStream = function(options){
  var options = this.getOptions(options);
   var path = [options.path,(options || {}).core || options.core,'update/json?commit='+ this.autoCommit +'&wt=json']
      .filter(function(element){
         if(element) return true;
         return false;    
      })
      .join('/');
   var headers = {
      'content-type' : 'application/json',
      'charset' : 'utf-8'
   };
   if(options.authorization){ 
      headers['authorization'] = options.authorization;
   }
   var optionsRequest = {
      url : 'http://' + options.host +':' + options.port + path ,
      method : 'POST',
      headers : headers
   };
   var jsonStreamStringify = JSONStream.stringify();
   var postRequest = request(optionsRequest);
   jsonStreamStringify.pipe(postRequest);
   var duplex = duplexer(jsonStreamStringify,postRequest);
   return duplex ;
}

/**
 * Commit last added and removed documents, that means your documents are now indexed.
 *
 * @param {Object} options 
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 *
 * @return {Client}
 * @api private
 */ 
 
Client.prototype.commit = function(options,callback){
   if(callback === undefined){
      callback = options;
      options = undefined;
   }
   var data = {};
   data['commit'] = {};
   options = this.getOptions(options);
   if( options !== undefined ){
      var availableAttributes = ['waitFlush','waitSeacher'];
      for ( var i = 0; i < availableAttributes.length ; i++){
         if ( options[availableAttributes [i]] !== undefined ){
            data['commit'][availableAttributes[i]] = options[availableAttributes[i]];
         }
      }
   }
   this.update(data,options,callback);
}

/**
 * Delete documents based on the given `field` and `text`.
 *
 * @param {String} field
 * @param {String} text
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 *
 * @return {Client}
 * @api public
 */ 
 
Client.prototype.delete = function(field,text,callback) {
   var self = this;
   text = format.dateISOify(text);
   var data = {};
   data['delete'] =  {query : field +  ':'  + text};
   this.update(data,{},callback);
   return self;
}

/**
 * Delete a range of documents based on the given `field`, `start` and `stop` arguments.
 *
 * @param {String} field
 * @param {String|Date} start
 * @param {String|Date} stop
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 *
 * @return {Client}
 * @api public
 */ 
 
Client.prototype.deleteByRange = function(field,start,stop,callback){
   var self = this;
   start = format.dateISOify(start);
   stop = format.dateISOify(stop);
   var data = {};
   data['delete'] = { query : field + ':[' + start + ' TO ' + stop + ']' };
   this.update(data,{},callback);
   return self;
}

/**
 * Delete the document with the given `id`
 *
 * @param {String|Number} id - id of the document you want to delete
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 *
 * @return {Client}
 * @api public
 */ 
 
Client.prototype.deleteByID = function(id,callback){
   var self = this;
   var data = {};
   data['delete'] =  {id : id.toString()};
   this.update(data,{},callback); 
   return self;
}

/**
 * Delete documents matching the given `query`
 *
 * @param {String} query - 
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 *
 * @return {Client}
 * @api public
 */

Client.prototype.deleteByQuery = function(query,callback){
   var self = this;
   var data = {};
   data['delete'] =  {query : query};
   this.update(data,{},callback);
   return self;
}
 
/**
 * Optimize the index
 *
 * @param {Object} options - 
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 *
 * @return {Client}
 * @api public
 */ 
 
Client.prototype.optimize = function(options,callback){
   if(callback === undefined){
      callback = options;
      options = undefined;
   }
   var data = {};
   data['optimize'] = {};
   if( options !== undefined ){
      var availableAttributes = ['waitFlush','waitSearcher'];
      for ( var i = 0; i < availableAttributes.length ; i++){
         if ( options[availableAttributes [i]] !== undefined ){
            data['optimize'][availableAttributes[i]] = options[availableAttributes[i]];
         }
      }
   }
   this.update(data,options,callback);
}

/**
 * Rollback all add/delete commands made since the last commit.
 *
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 * 
 * @return {Client}
 * @api public
 */
 
Client.prototype.rollback = function(callback){
   var data = {};
   data['rollback'] = {};
   this.update(data,{},callback);
}

/**
 * Send an update command to the Solr server with the given `data` stringified in the body.
 *
 * @param {Object} data - data sent to the Solr server
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 *
 * @return {Client}
 * @api private
 */
  
Client.prototype.update = function(data,options,callback){
   var self = this;
   if (!callback) {
    callback = options;
    options = {};
   }
   options = this.getOptions(options);
   if (!options.extract) {
     var json = [];
     if (Array.isArray(data)) {
       data.forEach(function (doc) {
         json.push('"add":' + JSON.stringify({doc:doc}));
       });
       if (options.autoCommit || this.autoCommit) {
         json.push('"commit":{waitSearcher:true}');
       }
       options.json = '{' + json.join(',') + '}';
     } else if (data['delete']) {
       var json = [];
       json.push('"delete":' + JSON.stringify(data['delete']));
       if (options.autoCommit || this.autoCommit) {
         json.push('"commit":{waitSearcher:true}');
       }
       options.json = '{' + json.join(',') + '}';
     } else {
       options.json = JSON.stringify(data);
     }
     options.extraHeaders = {'content-type': 'application/json'};
   } else
     options.stream = data;
   var url = (options.update || 'update');
   if (url.indexOf('?') == -1)
     url = url + '?commit='+ (options.autoCommit || this.autoCommit);
   options.fullPath = [options.path,options.core, url]
                              .filter(function(element){
                                 if(element) return true;
                                 return false;
                              })
                              .join('/');
   this.updateRequest(options,callback);
   return self;
}

/**
 * Search documents matching the `query`
 * 
 * @param {Query|String} query 
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 *
 * @return {Client}
 * @api public
 */ 

Client.prototype.search = function(query,callback){
   var self = this;
   // Allow to be more flexible allow query to be a string and not only a Query object
   var parameters = query.build ? query.build() : query;
   var options = this.getOptions();
   options.fullPath = [options.path, query._core || options.core,'select?' + parameters + '&wt=json']
                              .filter(function(element){
                                 if(element) return true;
                                 return false;    
                              })
                              .join('/'); ;
   this.queryRequest(options,callback);
   return self;
}

/**
 * Create an instance of `Query`
 *
 * @return {Query}
 * @api public
 */
 
Client.prototype.createQuery = function(){
   return new Query();
}


/**
 * Ping the Solr server
 *
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 *
 * @return {Client}
 * @api public
 */
 
Client.prototype.ping = function(callback){
   var self = this;
   var options = this.getOptions();
   options.fullPath = [options.path,options.core,'admin/ping?wt=json']
                              .filter(function(element){
                                 if(element) return true;
                                 return false;    
                              })
                              .join('/');
   this.queryRequest(options,callback);
   return self;
}

/**
 * HTTP POST request. Send update commands to the Solr server (commit, add, delete, optimize)
 * 
 * @param {Object} params
 * @param {String} params.host - IP address or host address of the Solr server
 * @param {Number|String} params.port - port of the Solr server
 * @param {String} params.core - name of the Solr core requested
 * @param {String} params.authorization - value of the authorization header
 * @param {String} params.fullPath - full path of the request
 * @param {String} params.json
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 *
 * @api private
 */
 
Client.prototype.updateRequest = function(params,callback){
   var headers = {
      'content-type' : 'application/json',
      'charset' : 'utf-8',
      'content-length':  params.json ? Buffer.byteLength(params.json) : undefined,
   };
   if(params.authorization){ 
      headers['authorization'] = params.authorization;
   }
   if (params.extraHeaders) {
      Object.keys(params.extraHeaders).forEach(function (e) {
         headers[e] = params.extraHeaders[e];
      });
   }
   if (!params.host) {
    var host = this.getHost().split(':');
    params.host = host[0];
    params.port = host[1] || 8983;
   }
   var options = {
      host : params.host,
      port : params.port,
      method : 'POST',
      headers : headers,
      path : params.fullPath 
   };
   var callbackResponse = function(res){
      var buffer = '';
      var err = null;
      res.on('data',function(chunk){
         buffer += chunk;
      });
      
      res.on('end',function(){
         if(res.statusCode !== 200){
            err = new SolrError(res.statusCode,buffer);
            if (callback)  callback(err,null);
         }else{
            if (params.extract) {
              var data = buffer;
              if (callback)  callback(err, data);
            } else {
              var data = null;
              try{
                 data = JSON.parse(buffer);
              }catch(error){
                 err = error;
              }finally{
                 if (callback)  callback(err, data || buffer);
              }
            }
         }
      });
   }
   
   var request = http.request(options,callbackResponse);
   
   request.on('error',function(err){
      if (callback) callback(err,null);
   });
   if (params.extract)
     request.write(params.stream);
   else
     request.write(params.json);
   request.end();
}

/**
 * HTTP GET request.  Send a query command to the Solr server (query)
 * 
 * @param {Object} params
 * @param {String} params.host - IP address or host address of the Solr server
 * @param {Number|String} params.port - port of the Solr server
 * @param {String} params.core - name of the Solr core requested
 * @param {String} params.authorization - value of the authorization header
 * @param {String} params.fullPath - full path of the request, contains query parameters
 * @param {Function} callback(err,obj) - a function executed when the Solr server responds or an error occurs
 * @param {Error} callback().err
 * @param {Object} callback().obj - JSON response sent by the Solr server deserialized
 * 
 * @api private
 */

Client.prototype.queryRequest = function(params,callback){
  var host = this.getHost();
  var self = this;
  function tryAgain() {
    host = self.getHost();
    var h = host.split(':');
    var options = {
      host : h[0],
      port : h[1] || 8983,
      path : params.fullPath
    };
   
    if(params.authorization){ 
      var headers = {
        'authorization' : params.authorization
      };
      options.headers = headers;
    }
   
    var callbackResponse = function(res){
      var buffer = '';
      var err = null;
      res.on('data',function(chunk){
        buffer += chunk;
      });
      
      res.on('end',function(){
        if(res.statusCode !== 200){
          try {
            buffer = JSON.parse(buffer);
          }catch(e) {
          }
          err = new SolrError(res.statusCode,buffer);
            if (callback)  callback(err,null);
        }else{
          try{
            var data = JSON.parse(buffer);
          }catch(error){
            err = error;
          }finally{
            if (callback)  callback(err,data);
          }
        }
      });
    }
   
    var request = http.get(options,callbackResponse);
    if (self.options.timeout)
      request.setTimeout(self.options.timeout);
    request.on('error', function(err) {
      if (self.options.hosts.length === 1) {
        return callback && callback(err);
      }
      var index = self.options.hosts.indexOf(host);
      if (index != -1)
        self.options.hosts.splice(index, 1);
      setTimeout(function retry() {
        self.options.hosts.push(host);
      }, 60000);
      if (self.options.hosts.length === 0)
        return callback && callback(err);
      tryAgain();
     });
   }
   tryAgain();
}

/**
  * HTTP POST requests. Send stuff to solr
  *
  */
Client.prototype.sendToSolr = function (options, callback) {
  var self = this;
  var headers = {
    'content-type' : options.contentType || 'text/xml',
    'charset' : 'utf-8'
  };
  if(options.authorization){ 
     headers['authorization'] = options.authorization;
  }
  var nullCallback = function () { };
  if (!callback) callback = nullCallback;
  function tryAgain() {
    var host = self.getHost();
    var optionsRequest = {
       url : 'http://' + host + self.options.path + options.path,
       method : options.method || 'POST',
       headers : headers,
    };
    if (self.options.timeout)
      optionsRequest.timeout = self.options.timeout;
    var postRequest = request(optionsRequest);
    postRequest.on('error', function (err) {
      if (self.options.hosts.length === 1) {
        if (callback) {
          callback(err);
          callback = nullCallback;
        }
        return;
      }
      var index = self.options.hosts.indexOf(host);
      if (index != -1)
        self.options.hosts.splice(index, 1);
      setTimeout(function retry() {
        self.options.hosts.push(host);
      }, 60000);
      if (self.options.hosts.length === 0) {
        if (callback) {
          callback(err);
          callback = nullCallback;
          return;
        }
      }
      tryAgain();
    });
    postRequest.on('response', function (response) {
      var result = "";
      response.on('data', function (chunk) {
        result += chunk;
      });
      response.on('end', function (err) {
        if (response.statusCode !== 200)
          callback(new Error('Status code was ' + response.statusCode), result);
        else
          callback(null, result);
        callback = nullCallback;
      });
    });
    if (options.content)
      postRequest.write(options.content);
    postRequest.end();
  }
  tryAgain();
}
