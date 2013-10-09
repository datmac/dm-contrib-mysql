#!/usr/bin/env node

var manifest = require('./manifest.json'),
    command = require('./index.js'),
    options = require('dm-core').cli(manifest)
;
command(options, process.stdin).pipe(process.stdout);
