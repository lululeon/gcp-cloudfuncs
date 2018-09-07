const mysql = require('mysql');
const storage = require('@google-cloud/storage');
const streamToString = require('stream-to-string');

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


const getQuery = (sqlfile => {
  return new Promise((resolve, reject) => {
    return storage.bucket(process.env.SQLBUCKET).getFiles()
    .then(results => {
      const files = results[0];
      const foundfile = files.find(f => {
        return f.name === sqlfile;
      });
      const rstream = foundfile.createReadStream();
      return streamToString(rstream);
    });
  });
});

const writeToCloudSQL = () => {
  const pool = mysql.createPool({
    connectionLimit : 1, //best practice.
    socketPath: '/cloudsql/' + process.env.CONNECTIONNAME,
    user: process.env.DBUSER,
    password: process.env.DBPASS,
    database: process.env.DBNAME
  });

  getQuery('create.sql')
  .then(sqlstr => {
    if(!sqlstr) {
      throw(new Error('no sql to execute'));
    }

    pool.query(sqlstr, (error, results, fields) => {
      if(error) {
        console.log(`could not execute query.`);
      } else {
        console.log(results);
      }
    });
  })
  .catch(err => {
    console.log('getQuery failed');
  });
}

module.exports = {
  etlIntakeDispatcher
}