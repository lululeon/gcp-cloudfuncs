const mysql = require('mysql');
const { Storage } = require('@google-cloud/storage');
const streamToString = require('stream-to-string');
const Json2csvTransform = require('json2csv').Transform;
const etl = require('node-etl');

/* ---- utils ---- */
const findStorageObject = (bucketName, filename) => {
  const storage = new Storage({
    projectId: process.env.PROJECTID
  });
  return storage.bucket(bucketName).getFiles()
  .then(results => {
    const files = results[0];
    const foundfile = files.find(f => {
      //presumes bucket has versioning off.
      return f.name === filename;
    });
  });
}

const getQuery = (sqlfile => {
  return findStorageObject(process.env.SQLBUCKET, sqlfile).then(foundfile => {
    const rstream = foundfile.createReadStream();
    return streamToString(rstream);
  });
});

const runQueryHelper = (sqlstr, pool) => {
  return new Promise((resolve, reject) => {
    pool.query(sqlstr, (error, results, fields) => {
      if(error) {
        console.log(`could not execute query.`);
        reject(error);
      } else {
        resolve(results);
      }
    });
  });
}

/* ---- stream transformers ---- */
const trxJSON2SQL = (loadedFile) => {
  return new Promise((resolve, reject) => {
    const pool = mysql.createPool({
      connectionLimit : 1, //best practice.
      socketPath: '/cloudsql/' + process.env.CONNECTIONNAME,
      user: process.env.DBUSER,
      password: process.env.DBPASS,
      database: process.env.DBNAME
    });
    const opts = { flatten:true, fields: [
      'base',
      'date', 
      { label: 'USD', value:'rates.USD'},
      { label: 'GBP', value:'rates.GBP'},
      { label: 'EUR', value:'rates.EUR'},
      { label: 'CNY', value:'rates.CNY'},
      { label: 'RUB', value:'rates.RUB'},
    ] };
    const transformOpts = {};
    const json2csv = new Json2csvTransform(opts, transformOpts);
    const rstream = loadedFile.createReadStream();
    rstream.pipe(json2csv)
    .pipe(etl.collect(1000))
    .pipe(etl.mysql.upsert(pool, process.env.DBNAME, 'rates'))
    .promise()
    .then(result => {
      console.log('transform complete!');
      resolve(result);
    })
    .catch(err => {
      reject(err);
    })
  });
}

/* ---- jobs ---- */
const importData = (bucketName, filename) => {
  console.log(typeof bucketName);
  const storage = new Storage({
    projectId: process.env.PROJECTID
  });
  const loadedFile = storage.bucket(bucketName).file(filename);
  trxJSON2SQL(loadedFile)
  .then(result => {
    console.log('import complete!');
  })
  .catch(err => {
    console.log('import failed:', err);
  });
}

/* ---- dispatchers ---- */
/**
 * Background Cloud Function (Storage)
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
    importData(file.bucket, file.name);
  } else {
    console.log(`File ${file.name} metadata updated.`);
    // fire update triggers
  }
};

/**
 * Background Cloud Function (PubSub)
 *
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
const etlRunQuery = (data, context) => {
  const pool = mysql.createPool({
    connectionLimit : 1, //best practice.
    socketPath: '/cloudsql/' + process.env.CONNECTIONNAME,
    user: process.env.DBUSER,
    password: process.env.DBPASS,
    database: process.env.DBNAME,
    multipleStatements: true //naughty. for now.
  });

  const sqlfilename = data.sqlfile;
  console.log(`Preparing to run sql from [${sqlfilename}]`);
  getQuery(sqlfilename)
  .then(sqlstr => {
    console.log(sqlstr);
    return runQueryHelper(sqlstr, pool);
  })
  .then(result => {
    console.log('etlRunQuery call ok', result);
  })
  .catch(err => {
    console.log('etlRunQuery failed:', err);
  });
};

module.exports = {
  etlIntakeDispatcher,
  etlRunQuery
}