const path = require('path');
const pull = require('pull-stream');
const FlumeLog = require('flumelog-offset');
const AsyncFlumeLog = require('async-flumelog');
const bipf = require('bipf');
const jsonCodec = require('flumecodec/json');
const debug = require('debug')('ssb:db2:migration');

const blockSize = 64 * 1024;

exports.init = function (_sbot, config) {
  const oldLogPath = path.join(config.path, 'flume', 'log.offset');
  const newLogPath = path.join(config.path, 'db2', 'log.bipf');

  const oldLog = FlumeLog(oldLogPath, {blockSize, codec: jsonCodec});
  const newLog = AsyncFlumeLog(newLogPath, {blockSize});

  let dataTransferred = 0;

  pull(
    oldLog.stream({seqs: false, codec: jsonCodec}),
    // oldLog.stream({seqs: false, live: true, codec: jsonCodec}), // FIXME: live
    pull.map(function (data) {
      const len = bipf.encodingLength(data);
      const buf = Buffer.alloc(len);
      bipf.encode(data, buf, 0);
      return buf;
    }),
    function sink(read) {
      read(null, function next(err, data) {
        if (err && err !== true) throw err;
        if (err) {
          debug('done');
          if (config._db2migrationCB) newLog.onDrain(() => {
            newLog.close(config._db2migrationCB)
          })
          return
        }
        dataTransferred += data.length;
        newLog.append(data, () => {});
        if (dataTransferred % blockSize == 0)
          newLog.onDrain(function () {
            read(null, next);
          });
        else read(null, next);
      });
    },
  );
};
