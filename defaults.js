// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const path = require('path')

exports.BLOCK_SIZE = 64 * 1024
exports.flumePath = (dir) => path.join(dir, 'flume')
exports.oldLogPath = (dir) => path.join(dir, 'flume', 'log.offset')
exports.newLogPath = (dir) => path.join(dir, 'db2', 'log.bipf')
exports.indexesPath = (dir) => path.join(dir, 'db2', 'indexes')
exports.resetLevelPath = (dir) =>
  path.join(dir, 'db2', 'post-compact-reset-level')
exports.resetPrivatePath = (dir) =>
  path.join(dir, 'db2', 'post-compact-reset-private')
exports.jitIndexesPath = (dir) => path.join(dir, 'db2', 'jit')
exports.tooHotOpts = (config) =>
  config.db2
    ? {
        ceiling: config.db2.maxCpu || Infinity,
        wait: config.db2.maxCpuWait || 90,
        maxPause: config.db2.maxCpuMaxPause || 300,
      }
    : { ceiling: Infinity, wait: 90, maxPause: 300 }
