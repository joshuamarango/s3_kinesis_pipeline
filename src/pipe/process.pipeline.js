'use strict';

const AWS = require('aws-sdk');
AWS.config.loadFromPath("./config.json");
const csv = require('fast-csv');

const 
    s3 = new AWS.S3(),
    fireHose = new AWS.Firehose();

const 
    { years, months } = require('../variables/variables'),
    ylen = years.length,
    mlen = months.length;

let myData = []


const dataPipeline = () => {

    for(let i = 0; i < ylen; i++){

        for(let j = 0; j < mlen; j++){

            const _i = i;
            let s3_params = { Bucket: "rilobucket", Key: `year=${years[_i]}/yellow_tripdata_${years[_i]}-${months[j]}.csv` };

            myData.push(s3_params);
        }

    }

    console.log(myData);

}

module.exports = {
    dataPipeline,
}