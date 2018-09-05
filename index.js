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
    // fire create triggers
  } else {
    console.log(`File ${file.name} metadata updated.`);
    // fire update triggers
  }
};

module.exports = {
  etlIntakeDispatcher
}