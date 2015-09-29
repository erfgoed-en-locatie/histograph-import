#!/usr/bin/env node

var fs = require('fs');
var path = require('path');
var url = require('url');
var request = require('request');
var async = require('async');
var _ = require('underscore');
var minimist = require('minimist');
var config = require('histograph-config');

var argv = minimist(process.argv.slice(2), {
  boolean: [
    'force',
    'clear'
  ]
});

var datasets = _.uniq(argv._);

var ignoredDirs = [
  'node_modules',
  '.git'
];

require('colors');

async.mapSeries(config.import.dirs, getDirectory, gotDirectories);

function getDirectory(dataDir, cb) {
  return async.waterfall([
    _.partial(fs.readdir, dataDir),
    filterFolders,
    _.partial(async.map, _, makeDirectoryObject),
  ], cb);

  function filterFolders(folders, cb){
    return async.filter( folders, validFolderPredicate, function( folders ){
      cb(null, folders);
    } );
  }

  function validFolderPredicate(dir, cb) {
    var dirIsLocalFolder = dir === '.',
        dirIsIgnored = ignoredDirs.indexOf(dir) > -1,
        dirPertainsToDataset = datasets.length === 0 || datasets.indexOf(dir) > -1;

    cb( !dirIsLocalFolder && !dirIsIgnored && dirPertainsToDataset );
  }

  function makeDirectoryObject(dir, cb){
    return cb(null, {
      id: dir,
      dir: path.join(dataDir, dir)
    });
  }

  function isDirectory(dataset, cb){
    return fs.stat(dataset.dir, function(err, stat){
      if(err) return cb(err);

      cb(null, stat.isDirectory());
    });
  }
}

function gotDirectories(err, dirs) {
  var notFound = datasets,
      dirs = _.flatten(dirs);

  return async.eachSeries( dirs, processDir, done );

  function processDir(dir, cb){
    if(datasets.length > 0){
      notFound.splice(notFound.indexOf(dir.id), 1);
    }

    importDatasetFromDir(dir, cb);
  }

  function done(err) {
    if (notFound.length > 0) {
      console.error('Dataset(s) not found in dirs `config.import.dirs`: '.red + notFound.join(', '));
    }
  }
}

function importDatasetFromDir(dataset, cb) {
  if (argv.clear) {
    return deleteDataset(dataset.id, function(err) {
      if (err) {
        console.error('Deleting dataset failed: '.red + err);
      } else {
        console.error('Deleted dataset: '.green + dataset.id);
      }

      cb();
    });
  }

  createDataset(dataset, function(err) {
    if (err) {
      console.error(('Creating dataset ' + dataset.id + ' failed: ').red + JSON.stringify(err));
      return cb();
    }

    console.error('Created or found dataset: '.green + dataset.id);
    uploadData(dataset, cb);
  });
}

function createDataset(dataset, cb) {
  var filename = path.join(dataset.dir, dataset.id + '.dataset.json');
  
  return async.waterfall([
    checkFileExistence,
    readFile,
    postFile
  ], cb);

  function checkFileExistence(cb){
    return fs.exists( filename, existsCb );

    function existsCb(exists){
      return cb(null, exists);
    }
  }

  function readFile(exists, cb){
    if(!exists) return cb('dataset JSON file `' + dataset.id + '.dataset.json` not found');

    fs.readFile(filename, 'utf8', cb);
  }

  function postFile(file, cb){
    return request(apiUrl('datasets'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: file
    }, responseHandler);

    function responseHandler(err, res, body){
      if(err) {
        return cb(err.message);
      }

      if( res.statusCode === 201 || res.statusCode === 409 ) {
        return cb();   
      }

      cb( JSON.parse( res.body ).message );
    }
  }

}

function deleteDataset(datasetId, cb) {
  return request( apiUrl( 'datasets/' + datasetId ), {
    method: 'DELETE'
  }, responseHandler);

  function responseHandler(err, res, body) {
    if (err) {
      return cb(err.message);
    }

    if (res.statusCode === 200) {
      cb();
    }

    cb( JSON.parse( res.body ).message );
  }
}

function apiUrl(path) {
  var urlObj = url.parse( config.api.baseUrl );
  
  urlObj.auth = config.api.admin.name + ':' + config.api.admin.password;
  urlObj.pathname = path;

  return url.format( urlObj );
}

function uploadData(dataset, cb) {
  var files = [
    'pits',
    'relations'
  ];

  return async.eachSeries(files, uploadFile, cb);

  function uploadFile(file, cb) {
    var filename = path.join(dataset.dir, dataset.id + '.' + file + '.ndjson');
    var base = path.basename(filename);

    return async.waterfall([
      checkExistence,
      putFile
    ], done);

    function checkExistence(cb){
      return fs.exists(filename, existsCb);

      function existsCb(exists){
        return cb(null, exists);
      }
    }

    function putFile(exists, cb){
      if(!exists){
        console.log('File not found: '.yellow + base);
        return cb();
      }

      var formData = { file: fs.createReadStream( filename ) };

      request.put( apiUrl( 'datasets/' + dataset.id + '/' + file ), {
        formData: formData,
        headers: {
          'content-type': 'application/x-ndjson',
          'x-histograph-force': argv.force
        }
      }, cb);
    }

    function done(err, res, body) {
      if (err) {
        console.error('Upload failed: '.red + base);
        console.error('\t' + err.code);
      } else if (res && res.statusCode == 200) {
        console.log('Upload successful: '.green + base);
      } else {
        var message;
        try {
          message = JSON.parse(body);
        } catch (parseError) {
          message = { message: body };
        }

        console.log('err', message);
        console.log('Upload failed: '.red + base);

        if (message.details) {
          console.log(JSON.stringify(message, null, 2).split('\n').map(function(line) {
            return '\t' + line;
          }).join('\n'));
        } else {
          console.log(message.message);
        }
      }

      cb();
    }
  }
}
