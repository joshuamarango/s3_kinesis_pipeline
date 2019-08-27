'use strict';

const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

if (cluster.isMaster) {

    console.log('Master process is running with pid:', process.pid);

    for (let i = 0; i < numCPUs; ++i) {

        cluster.fork();

    }
    
} else if(cluster.worker.id == 1) {

    // Begin streaming here

    console.log('First worker', process.pid);

} else if(cluster.worker.id == 2) {

    console.log('Second worker', process.pid);
} else {


    console.log("other workers")

}