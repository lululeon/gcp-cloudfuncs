// const fs = require("fs");
// const path = require("path");
const mysql = require('mysql');

/**
 * Background Cloud Function.
 *
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
const etlIntakeDispatcher = (data, context) => {
  const file = data; //storage obj.

  //we don't care about this signal
  if (file.resourceState === 'not_exists') {
    console.log(`File ${file.name} deleted.`);
    return;
  }

  if (file.metageneration === '1') {
    console.log(`File ${file.name} uploaded.`);
    writeToCloudSQL();
  } else {
    console.log(`File ${file.name} metadata updated.`);
    // fire update triggers
  }
};


const getQuery = (queryName => {
  const storageObject = `${process.env.BUCKET}/${queryName}`;
  //TODO
});

const runQuery = (queryName => {
  const query = getQuery(queryName);
  if(query === '') {
    console.log('empty query.');
    return;
  }
});

const writeToCloudSQL = () =>{
  const pool = mysql.createPool({
    connectionLimit : 1, //best practice.
    socketPath: '/cloudsql/' + process.env.CONNECTIONNAME,
    user: process.env.DBUSER,
    password: process.env.DBPASS,
    database: process.env.DBNAME
  });

  const queryStrings = `
    USE cadrates;
    CREATE TABLE IF NOT EXISTS rates
    (
      source varchar(50),
      fetchdate date,
      gbp float,
      eur float,
      usd float,
      cny float,
      PRIMARY KEY (date)
    );
    ALTER TABLE rates ADD INDEX (date);
  `;

  pool.query(queryStrings, (error, results, fields) => {
    if(error) {
      console.log(`query with name [${queryStrings}] failed.`)
    } else {
      console.log(results);
    }
  });
}

module.exports = {
  etlIntakeDispatcher
}