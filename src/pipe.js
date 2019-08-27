'use strict';

const cluster = require('cluster');
const numCPUs = require('os').cpus().length;
const { dataPipeline } = require('./pipe/process.pipeline');

if (cluster.isMaster) {
    console.log('Master process is running with pid:', process.pid);
    for (let i = 0; i < numCPUs; ++i) {
        cluster.fork();
    }
} else {
    // Distribute workload between workers(cpus)
    dataPipeline();
}