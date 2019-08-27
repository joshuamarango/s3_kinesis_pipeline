'use strict';

const AWS = require('aws-sdk');
AWS.config.loadFromPath("./config/config.json");
const csv = require('fast-csv');

const 
    s3 = new AWS.S3(),
    fireHose = new AWS.Firehose();

const 
    {
        b1,
        b2,
        b3,
        b4,
        b5,
        b6,
        months,
        months_2019
    } = require('../variables/variables'),
    b1len = b1.length,
    b2len = b2.length,
    b3len = b3.length,
    b4len = b4.length,
    b5len = b5.length,
    b6len = b6.length,
    mlen = months.length,
    m9len = months_2019.length;


const batch_1 = () => {

    for(let i = 0; i < b1len; i++){

        for(let j = 0; j < mlen; j++){

            const _i = i;
            let s3_params = { Bucket: "rilobucket", Key: `year=${b1[_i]}/yellow_tripdata_${b1[_i]}-${months[j]}.csv` };

            let s3_stream = s3.getObject(s3_params).createReadStream();
            csv.fromStream(s3_stream)
                .on('data', (x) => {
                    let output = {
                        vendor_id: x[0],
                        pickup_datetime: x[1],
                        dropoff_datetime: x[2],
                        passenger_count: x[3],
                        trip_distance: x[4],
                        pickup_locationId: `[${x[5]},${x[6]}]`,
                        dropoff_locationId: `[${x[9]},${x[10]}]`,
                        rate_code_id: x[7],
                        store_and_forward_id: x[8],
                        payment_type: x[11],
                        fare_amount: x[12],
                        surcharge: x[13],
                        mta_tax: x[14],
                        tip_amount: x[15],
                        tolls_amount: x[16],
                        congestion_surcharge: 0
                    };
                    /*
                    let fireHose_params = {
                        Data: new Buffer(JSON.stringify(output)),
                        StreamName: 'StreamName'
                    };
                    fireHose.putRecord(fireHose_params, (err,data) => {
                        console.log(err,data)
                    });
                    */
                    console.log(JSON.stringify(output)) // test if output is being returned
                })
                .on('end', () => { console.log("Batch 1 has finished being processed") });
        }

    }
};

const batch_2 = () => {

    for(let i = 0; i < b2len; i++){

        for(let j = 0; j < mlen; j++){

            const _i = i;
            let s3_params = { Bucket: "rilobucket", Key: `year=${b2[_i]}/yellow_tripdata_${b2[_i]}-${months[j]}.csv` };

            let s3_stream = s3.getObject(s3_params).createReadStream();
            csv.fromStream(s3_stream)
                .on('data', (x) => {
                    let output = {
                        vendor_id: x[0],
                        pickup_datetime: x[1],
                        dropoff_datetime: x[2],
                        passenger_count: x[3],
                        trip_distance: x[4],
                        pickup_locationId: `[${x[5]},${x[6]}]`,
                        dropoff_locationId: `[${x[9]},${x[10]}]`,
                        rate_code_id: x[7],
                        store_and_forward_id: x[8],
                        payment_type: x[11],
                        fare_amount: x[12],
                        surcharge: x[13],
                        mta_tax: x[14],
                        tip_amount: x[15],
                        tolls_amount: x[16],
                        congestion_surcharge: 0
                    };
                    /*
                    let fireHose_params = {
                        Data: new Buffer(JSON.stringify(output)),
                        StreamName: 'StreamName'
                    };
                    fireHose.putRecord(fireHose_params, (err,data) => {
                        console.log(err,data)
                    });
                    */
                    console.log(JSON.stringify(output)) // test if output is being returned
                })
                .on('end', () => { console.log("Batch 2 has finished being processed") });
        }

    }
};

const batch_3 = () => {

    for(let i = 0; i < b3len; i++){

        for(let j = 0; j < mlen; j++){

            const _i = i;
            let s3_params = { Bucket: "rilobucket", Key: `year=${b3[_i]}/yellow_tripdata_${b3[_i]}-${months[j]}.csv` };

            let s3_stream = s3.getObject(s3_params).createReadStream();
            csv.fromStream(s3_stream)
                .on('data', (x) => {
                    let output = {
                        vendor_id: x[0],
                        pickup_datetime: x[1],
                        dropoff_datetime: x[2],
                        passenger_count: x[3],
                        trip_distance: x[4],
                        pickup_locationId: `[${x[5]},${x[6]}]`,
                        dropoff_locationId: `[${x[9]},${x[10]}]`,
                        rate_code_id: x[7],
                        store_and_forward_id: x[8],
                        payment_type: x[11],
                        fare_amount: x[12],
                        surcharge: x[13],
                        mta_tax: x[14],
                        tip_amount: x[15],
                        tolls_amount: x[16],
                        congestion_surcharge: 0
                    };
                    /*
                    let fireHose_params = {
                        Data: new Buffer(JSON.stringify(output)),
                        StreamName: 'StreamName'
                    };
                    fireHose.putRecord(fireHose_params, (err,data) => {
                        console.log(err,data)
                    });
                    */
                    console.log(JSON.stringify(output)) // test if output is being returned
                })
                .on('end', () => { console.log("Batch 3 has finished being processed") });
        }

    }
};

const batch_4 = () => {

    for(let i = 0; i < b4len; i++){

        for(let j = 0; j < mlen; j++){

            const _i = i;
            let s3_params = { Bucket: "rilobucket", Key: `year=${b4[_i]}/yellow_tripdata_${b4[_i]}-${months[j]}.csv` };

            let s3_stream = s3.getObject(s3_params).createReadStream();
            csv.fromStream(s3_stream)
                .on('data', (x) => {
                    let output = {
                        vendor_id: x[0],
                        pickup_datetime: x[1],
                        dropoff_datetime: x[2],
                        passenger_count: x[3],
                        trip_distance: x[4],
                        pickup_locationId: `[${x[5]},${x[6]}]`,
                        dropoff_locationId: `[${x[9]},${x[10]}]`,
                        rate_code_id: x[7],
                        store_and_forward_id: x[8],
                        payment_type: x[11],
                        fare_amount: x[12],
                        surcharge: x[17],
                        mta_tax: x[14],
                        tip_amount: x[15],
                        tolls_amount: x[16],
                        congestion_surcharge: 0
                    };
                    /*
                    let fireHose_params = {
                        Data: new Buffer(JSON.stringify(output)),
                        StreamName: 'StreamName'
                    };
                    fireHose.putRecord(fireHose_params, (err,data) => {
                        console.log(err,data)
                    });
                    */
                   console.log(JSON.stringify(output)) // test if output is being returned
                })
                .on('end', () => { console.log("Batch 4 has finished being processed") });
        }

    }
};

const batch_5 = () => {

    for(let i = 0; i < b5len; i++){

        for(let j = 0; j < mlen; j++){

            const _i = i;
            let s3_params = { Bucket: "rilobucket", Key: `year=${b5[_i]}/yellow_tripdata_${b5[_i]}-${months[j]}.csv` };

            let s3_stream = s3.getObject(s3_params).createReadStream();
            csv.fromStream(s3_stream)
                .on('data', (x) => {
                    let output = {
                        vendor_id: x[0],
                        pickup_datetime: x[1],
                        dropoff_datetime: x[2],
                        passenger_count: x[3],
                        trip_distance: x[4],
                        pickup_locationId: x[7],
                        dropoff_locationId: x[8],
                        rate_code_id: x[5],
                        store_and_forward_id: x[6],
                        payment_type: x[9],
                        fare_amount: x[10],
                        surcharge: x[15],
                        mta_tax: x[12],
                        tip_amount: x[13],
                        tolls_amount: x[14],
                        congestion_surcharge: 0
                    };
                    /*
                    let fireHose_params = {
                        Data: new Buffer(JSON.stringify(output)),
                        StreamName: 'StreamName'
                    };
                    fireHose.putRecord(fireHose_params, (err,data) => {
                        console.log(err,data)
                    });
                    */
                   console.log(JSON.stringify(output)) // test if output is being returned
                })
                .on('end', () => { console.log("Batch 5 has finished being processed") });
        }

    }
};

const batch_6 = () => {

    for(let i = 0; i < b6len; i++){

        for(let j = 0; j < m9len; j++){

            const _i = i;
            let s3_params = { Bucket: "rilobucket", Key: `year=${b6[_i]}/yellow_tripdata_${b6[_i]}-${months_2019[j]}.csv` };

            let s3_stream = s3.getObject(s3_params).createReadStream();
            csv.fromStream(s3_stream)
                .on('data', (x) => {
                    let output = {
                        vendor_id: x[0],
                        pickup_datetime: x[1],
                        dropoff_datetime: x[2],
                        passenger_count: x[3],
                        trip_distance: x[4],
                        pickup_locationId: x[7],
                        dropoff_locationId: x[8],
                        rate_code_id: x[5],
                        store_and_forward_id: x[6],
                        payment_type: x[9],
                        fare_amount: x[10],
                        surcharge: x[15],
                        mta_tax: x[12],
                        tip_amount: x[13],
                        tolls_amount: x[14],
                        congestion_surcharge: x[17]
                    };
                    /*
                    let fireHose_params = {
                        Data: new Buffer(JSON.stringify(output)),
                        StreamName: 'StreamName'
                    };
                    fireHose.putRecord(fireHose_params, (err,data) => {
                        console.log(err,data)
                    });
                    */
                   console.log(JSON.stringify(output)) // test if output is being returned
                })
                .on('end', () => { console.log("Batch 6 has finished being processed") });
        }

    }
};

module.exports = {
    batch_1,
    batch_2,
    batch_3,
    batch_4,
    batch_5,
    batch_6,
};