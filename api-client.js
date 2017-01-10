var util = require('util');
var url = require('url');
var request = require('request');
var config = require('histograph-config');

var apiUrl = function(path) {
  var urlObj = url.parse(config.api.baseUrl);
  urlObj.auth = config.api.admin.name + ':' + config.api.admin.password;
  urlObj.pathname = path;
  console.log("URL: " + url.format(urlObj));
  return url.format(urlObj);
};

module.exports.createDataset = function(dataset, callback) {
  console.log("Body: " + JSON.stringify(dataset));
  request(apiUrl('datasets'), {
    method: 'POST',
    headers: {
      'content-type': 'application/json'
    },
    body: JSON.stringify(dataset)
  }, callback);
};

module.exports.deleteDataset = function(datasetId, callback) {
  request(apiUrl('datasets/' + datasetId), {
    method: 'DELETE'
  }, callback);
};

module.exports.uploadData = function(datasetId, type, readStream, size, force, callback) {
  request.put(apiUrl('datasets/' + datasetId + '/' + type), {
    formData: {
      file: {
        value: readStream,
        options: {
          filename: util.format('%s.%s.ndjson', datasetId, type),
          contentType: 'application/x-ndjson',
          knownLength: size
        }
      }
    },
    headers: {
      'content-type': 'application/x-ndjson',
      'x-histograph-force': force
    }
  }, callback);
};
