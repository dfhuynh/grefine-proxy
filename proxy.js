/**
 *
 *  Google Refine Proxy
 *
 */

var transformerEntryPoint = '/transformer';
var transformEntryPoint = '/transform';

var proxyConfig = {
  host: "127.0.0.1",
  port: 8080
};
var refineConfig = {
  host: '127.0.0.1',
  port: 3333
};

var events = require('events'),
    formidable = require('formidable'),
    fs = require('fs'),
    http = require('http'),
    nodestatic = require('node-static'),
    path = require('path');
    querystring = require('querystring'),
    timers = require('timers');

var log = function(s) {
  console.log(new Date() + ': ' + s);
};

var startsWith = function(s1, s2) {
  return s1.length >= s2.length && s1.substring(0, s2.length) == s2;
};

var parseParams = function(request) {
  var q = request.url.indexOf('?');
  if (q < 0) {
    return {};
  }
  return querystring.parse(request.url.substring(q + 1));
};

var isAcceptingJson = function(request) {
  var accepts = request.headers.accept.toLowerCase().replace(/,\s+/g, ',').split(',');
  for (var i = 0; i < accepts.length; i++) {
    var accept = accepts[i];
    if (accept == 'application/json' ||
        accept == 'application/jsonp' ||
        accept == 'text/javascript') {
      return true;
    }
  }
  return false;
};

var respondErrorText = function(request, response, statusCode, body) {
  if (isAcceptingJson(request)) {
    var jsonBody = '';
    var params = parseParams(request);
    var isJsonp = ('callback' in params);
    if (isJsonp) {
      jsonBody += params.callback + '(';
    }
    jsonBody += JSON.stringify({
      "code" : "error",
      "status" : statusCode,
      "message" : body
    });
    if (isJsonp) {
      jsonBody += ')';
    }

    response.writeHead(200, {
      'Content-Type' : isJsonp ? 'text/javascript' : 'application/json',
      'Content-Length' : jsonBody.length
    });
    response.write(jsonBody);
  } else {
    response.writeHead(statusCode, {
      'Content-Type' : 'text/plain',
      'Content-Length' : body.length
    });
    response.write(body);
  }
  response.end();
};

var shouldPassRequestThru = function(path) {
  if (startsWith(path, "/command/")) {
    var segments = path.substring("/command/".length).split("/");
    if (segments[0] == "core") {
      var command = segments[1];
      if (startsWith(command, "get-") ||
          startsWith(command, "compute-") ||
          startsWith(command, "preview-")) {
        return true;
      }
    }
    return false;
  } else {
    return true;
  }
};

var refineConnection = null;

var rawCreatePassThruRequest = function(method, url, headers) {
  if (refineConnection == null) {
    refineConnection = http.createClient(refineConfig.port, refineConfig.host);
  }
  return refineConnection.request(method, url, headers);
};

var createPassThruRequest = function(method, url, headers) {
  // Try twice but ignore exceptions if we don't succeed.
  for (var i = 0; i < 2; i++) {
    try {
      return rawCreatePassThruRequest(method, url, headers);
    } catch (e) {
      refineConnection = null;
      // Ignore
    }
  }
  // Try the third time and if it fails, let the exception bubbles up.
  return rawCreatePassThruRequest(method, url, headers);
};

var passRequestThru = function (proxyReq, proxyRes) {
  var refineReq = createPassThruRequest(
    proxyReq.method,
    proxyReq.url,
    proxyReq.headers
  );
  
  refineReq.on('response', function(refineRes) {
    proxyRes.writeHead(refineRes.statusCode, refineRes.headers);
    refineRes.on('data', function(chunk) {
      proxyRes.write(chunk, 'utf-8');
    });
    refineRes.on('end', function() {
      proxyRes.end();
      log('Proxied ' + proxyReq.method + ': ' + proxyReq.url);
    });
  });

  proxyReq.on('data', function(chunk) {
    refineReq.write(chunk, 'binary');
  });
  proxyReq.on('end', function() {
    refineReq.end();
  });
};

var readSync = function(path, callback) {
  var fd = fs.openSync(path, 'r');
  var bufferSize = 4096;
  var position = 0;
  while (true) {
    var res = fs.readSync(fd, bufferSize, position);
    if (res[1] == 0) {  
      break;
    }  
    callback(res[0]);
    position += res[1];
  }
  fs.close(fd);
};

var createRefinePostRequest = function(options) {
  options.host = refineConfig.host;
  options.port = refineConfig.port;
  options.method = 'POST';
  
  if (!(options.headers)) {
    options.headers = {};
  }
  options.headers['Connection'] = 'keep-alive';
  options.headers['Referer'] = 'http://' + refineConfig.host + ':' + refineConfig.port + '/';
  options.headers['Cache-Control'] = 'max-age=0';
  options.headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.34 Safari/534.24';
  options.headers['Accept'] = 'application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5'
  //options.headers['Accept-Encoding'] = 'gzip,deflate,sdch';
  options.headers['Accept-Language'] = 'en-US,en;q=0.8';
  options.headers['Accept-Charset'] = 'ISO-8859-1,utf-8;q=0.7,*;q=0.3';
  
  var req = http.request(options);
  return req;
};

var createRefineMultipartPostRequest = function(options, onResponse) {
  var boundary = "----boundary" + new Date().getTime();
  if (!(options.headers)) {
    options.headers = {};
  }
  options.headers['Content-Type'] = 'multipart/form-data; boundary=' + boundary;
  options.headers['Transfer-Encoding'] = 'chunked';
  
  var req = createRefinePostRequest(options);
  req.on('response', onResponse);
  
  var write = function(chunk) {
    // In theory, s should be preceded with its length, like so:
    //   var s = chunk.length + '\r\n' + chunk + '\r\n';
    // But for some reason, the Java class used to process the request doesn't like that.
    var s = chunk;
    req.write(s);
  };
  var end = function() {
    // In theory, we need to end with:
    //   req.write('0\r\n');
    req.end();
  };
  
  var o = {};
  o.partBegin = function(contentType, name, fileName) {
    var headers = [
      'Content-Disposition: form-data',
      'name="' + name + '"'
    ];
    if (fileName) {
      headers.push('filename="' + fileName + '"');
    }
    write('--' + boundary + '\r\n' +
      headers.join('; ') + '\r\n' +
      'Content-Type: ' + contentType + '\r\n\r\n');
  };
  o.partData = function(data) {
    write(data);
  };
  o.partEnd = function() {
    write('\r\n');
  };
  o.fieldPart = function(name, value) {
    write(
      '--' + boundary +
      '\r\nContent-Disposition: form-data; name="' + name + '"\r\n\r\n' +
       value + '\r\n');
  };
  o.endAll = function() {
    write('--' + boundary + '--\r\n');
    end();
  };
  return o;
};

var createProject = function(proxyReq, callback) {
  var params = {};
  var operationChunks = [];
  var dataChunks = [];

  var filename = "";
  var filetype = "";


  var projectName = 'transform-' + new Date().toString();
  var postParams = [
    'project-name=' + escape(projectName),
    'url=',
    'split-into-columns=true',
    'separator=',
    'ignore=0',
    'header-lines=1',
    'skip=0',
    'limit=',
    'guess-value-type=true',
    'ignore-quotes=false'
  ];
  var createProjectReq = createRefineMultipartPostRequest(
    {
      path: '/command/core/create-project-from-upload?' + postParams.join('&')
    },
    function(createProjectRes) {
      var chunks = [];
      createProjectRes.on('data', function(chunk) {
        chunks.push(chunk);
      });
      createProjectRes.on('end', function() {
        var redirect = createProjectRes.headers.location;
        var projectID = redirect.split('=')[1];
        callback(
          params,
          operationChunks.join(''),
          projectID
        );
      });
    }
  );
  createProjectReq.fieldPart('project-name', projectName);

  var form = new formidable.IncomingForm();
  form.addListener('file', function(field, file) {
    if (field == 'operation-file') {
      readSync(file.path, function(data) { operationChunks.push(data); });
    } else if (field == 'data-file') {
      filename = file.filename;
      filetype = file.type;
      readSync(file.path, function(data) { dataChunks.push(data); });
    }
  });
  form.addListener('field', function(name, value) {
    switch(name){
      case "operations-string":
        operationChunks.push(value);
        break;
      case "data-string":
        dataChunks.push(value);
        break;
      case "data-filetype":
        filename = value;
        break;
      case "data-filename":
        filetype = value;
        break;
      default:
        params[name] = value;
        break;
    }
  });
  form.addListener('end', function() {
    createProjectReq.partBegin(filetype, 'project-file', filename);
    createProjectReq.partData(dataChunks.join(''))
    createProjectReq.partEnd();
    createProjectReq.endAll();
  });
  form.parse(proxyReq);
};

var waitUntilIdle = function(projectID, callback) {
  var poll = function() {
    options = {
      host: refineConfig.host,
      port: refineConfig.port,
      method: 'GET',
      path: '/command/core/get-processes?project=' + projectID
    };
    var req = http.request(options);
    req.on('response', function(res) {
      var chunks = [];
      res.on('data', function(chunk) { chunks.push(chunk); });
      res.on('end', function() {
        var result = JSON.parse(chunks.join(''));
        if ('processes' in result && result.processes.length > 0) {
          timers.setTimeout(poll, 1000);
        } else {
          callback();
        }
      });
    });
    req.end();
  };
  poll();
};

var applyOperations = function(projectID, operationJson, callback) {
  var body = 'operations=' + escape(operationJson);
  var req = createRefinePostRequest({
    path: '/command/core/apply-operations?project=' + projectID,
    headers: {
      'Content-Type' : 'application/x-www-form-urlencoded',
      'Content-Length' : body.length
    }
  });
  req.on('response', function(res) {
    var chunks = [];
    res.on('data', function(chunk) {
      chunks.push(chunk);
    });
    res.on('end', function() {
      var result = JSON.parse(chunks.join(''));
      if (result.code == 'pending') {
        waitUntilIdle(projectID, callback);
      } else {
        callback();
      }
    });
  });
  req.write(body);
  req.end();
};

var deleteProject = function(projectID) {
  var body = 'project=' + projectID;
  var req = createRefinePostRequest({
    path: '/command/core/delete-project',
    headers: {
      'Content-Type' : 'application/x-www-form-urlencoded',
      'Content-Length' : body.length
    }
  });
  req.on('response', function(res) {
    var chunks = [];
    res.on('data', function(chunk) {
      chunks.push(chunk);
    });
    res.on('end', function() {
      var json = JSON.parse(chunks.join(''));
      if ('code' in json && json.code == 'ok') {
        log('Deleted project ' + projectID);
      }
    });
  });
  req.write(body);
  req.end();
};

var doTransform = function(proxyReq, proxyRes) {
  createProject(proxyReq, function(params, operationJson, projectID) {
    log('Created project ' + projectID);
    
    applyOperations(projectID, operationJson, function() {
      log('Applied operations to project ' + projectID);
      
      var body = [
        'engine=' + escape('{"facets":[],"mode":"row-based"}'),
        'project=' + escape(projectID),
        'format=' + params['format'] || 'tsv'
      ].join("&");
      
      var exportReq = createRefinePostRequest({
        path: '/command/core/export-rows/' + projectID,
        headers: {
          'Content-Type' : 'application/x-www-form-urlencoded',
          'Content-Length' : body.length
        }
      });
      exportReq.on('response', function(exportRes) {
        proxyRes.writeHead(exportRes.statusCode, exportRes.headers);
        exportRes.on('data', function(chunk) {
          proxyRes.write(chunk, 'binary');
        });
        exportRes.on('end', function() {
          proxyRes.end();
          
          log('Exported project ' + projectID);
          
          deleteProject(projectID);
        });
      });
      exportReq.write(body);
      exportReq.end();
    });
  });
};

var staticServer = new nodestatic.Server('./content');
staticServer.resolve = function(pathname) {
  pathname = pathname.substring(transformerEntryPoint.length);
  return path.normalize(path.join(this.root, pathname));
};

http.createServer(function (proxyReq, proxyRes) {
  if (proxyReq.method != 'GET' && proxyReq.method != 'POST') {
    respondErrorText(proxyReq, proxyRes, 501, 'Unsupported method');
    return;
  }

  var url = proxyReq.url;
  var path = url.split('?')[0];
  if (path == transformerEntryPoint) {
    proxyRes.writeHead(302, {
      'Location' : 'http://' + proxyConfig.host + ':' + proxyConfig.port + transformerEntryPoint + '/',
      'Content-Length' : 0
    });
    proxyRes.end();
  } else if (startsWith(path, transformerEntryPoint + '/')) {
    staticServer.serve(proxyReq, proxyRes);
  } else if (proxyReq.method == 'POST' && (path == transformEntryPoint ||
             startsWith(path, transformEntryPoint + '/'))) {
    doTransform(proxyReq, proxyRes);
  } else if (shouldPassRequestThru(path)) {
    passRequestThru(proxyReq, proxyRes);
  } else {
    log('Blocked ' + proxyReq.method + ': ' + proxyReq.url);
    respondErrorText(proxyReq, proxyRes, 502, 'Unsupported request');
  }
}).listen(proxyConfig.port, proxyConfig.host);

(function() {
  var rootUrl = 'http://' + proxyConfig.host + ':' + proxyConfig.port;
  
  console.log('\nRead-only Google Refine is proxied at ' + rootUrl + '/');
  console.log('Transformer page is served at ' + rootUrl + transformerEntryPoint + '/');
  console.log('Transform POST entry point is at ' + rootUrl + transformEntryPoint);
  console.log('Ready.\n');
})();

