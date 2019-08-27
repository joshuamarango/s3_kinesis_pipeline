'use strict';

const cluster = require('cluster');
const numCPUs = require('os').cpus().length;
const 
    { 
        batch_1,
        batch_2,
        batch_3,
        batch_4,
        batch_5,
        batch_6
    } = require('./pipe/process.pipeline');

if (cluster.isMaster) {
    console.log('Master process is running with pid:', process.pid);
    for (let i = 0; i < numCPUs; ++i) {
        cluster.fork();
    }
} else if(cluster.worker.id == 1){ batch_1() }
  else if(cluster.worker.id == 2){ batch_2() }
  else if(cluster.worker.id == 3){ batch_3() }
  else if(cluster.worker.id == 4){ batch_4() }
  else if(cluster.worker.id == 5){ batch_5() }
  else if(cluster.worker.id == 6){ batch_6() }