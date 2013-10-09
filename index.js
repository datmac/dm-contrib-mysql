'use strict';

var path = require('path')
, basename = path.basename(path.dirname(__filename))
, debug = require('debug')('dm:contrib:' + basename)
, Transform = require("stream").Transform
, Segmenter = require('segmenter')
, mysql = require('mysql')
, async = require('async')
;

function notEmpty(x) {
  return (x !== '' && x !== undefined);
}

function Command(options)
{
  Transform.call(this, options);

  var self = this;
  self.begin = true;
  self.ended = false;
  self.seg = new Segmenter();
  self.counter = 0
  self.url = options.url || 'mysql://root:@localhost/test';
 
  var i = self.url.indexOf('mysql://');
  if (i === -1) {
    self.url = 'mysql://' + self.url;
  }
  else {
    self.url = self.url.substring(i);
  }
  self.pool = mysql.createPool(self.url);
}

Command.prototype = Object.create(
  Transform.prototype, { constructor: { value: Command }});

Command.prototype.parse = function (queries, done) {
  var self = this;

  self.pool.getConnection(function (err, connection) {
      if (err) {
        done()
        return;
      }
      async.mapSeries(queries.filter(notEmpty), function (sql, callback) {
          connection.query(sql, function (err, rows) {
              self.counter++;
              if (err) {
                callback(err, JSON.stringify(err));
              }
              else {
                callback(null, JSON.stringify(rows));
              }
            }
          );
        }, function (err, results) {
          self.push(results.join(',\n'));
          connection.release();
          done();
        }
      );
    }
  )
}


Command.prototype._transform = function (chunk, encoding, done) {
  var self = this;
  if (self.begin) {
    self.begin = false;
    self.emit('begin');
    self.push('[\n');
  }
  if (self.counter > 0) {
    self.push(',\n');
  }
  self.parse(self.seg.fetch(chunk, encoding), function () {
      if (self.ended) {
        self._flush(done);
      }
      else {
        done();
      }
    }
  );
}
Command.prototype._flush = function (done) {
  var self = this;
  self.parse(self.seg.fetch(), function () {
      self.pool.end();
      self.push('\n]');
      done();
    }
  );
}

Command.prototype.end = function () {
  var self = this;
  self.ended = true;
};

module.exports = function (options, si) {
  var cmd = new Command(options);
  return si.pipe(cmd);
}
