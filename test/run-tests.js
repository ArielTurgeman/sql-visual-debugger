const assert = require('node:assert/strict');

const suites = [
  require('./localHostPolicy.test'),
  require('./databaseDetection.test'),
  require('./queryExtractor.test'),
  require('./queryBlocks.test'),
  require('./mysqlRunner.test'),
  require('./webviewPanel.test'),
  require('./cteStep.test'),
  require('./fromStep.test'),
  require('./groupByStep.test'),
  require('./joinStep.test'),
  require('./subqueryStep.test'),
  require('./selectStep.test'),
  require('./windowStep.test'),
  require('./orderByStep.test'),
  require('./limitStep.test'),
  require('./whereStep.test'),
  require('./havingStep.test'),
];

let total = 0;
let failed = 0;
const pending = [];

function runTest(name, fn) {
  total += 1;
  try {
    const result = fn();
    if (result && typeof result.then === 'function') {
      const pendingResult = result.then(() => {
        console.log(`PASS ${name}`);
      }).catch((error) => {
        failed += 1;
        console.error(`FAIL ${name}`);
        console.error(error && error.stack ? error.stack : error);
      });
      pending.push(pendingResult);
      return pendingResult;
    }
    console.log(`PASS ${name}`);
  } catch (error) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(error && error.stack ? error.stack : error);
  }
}

(async () => {
  for (const suite of suites) {
    suite(runTest, assert);
  }

  await Promise.all(pending);

  console.log(`\n${total - failed}/${total} tests passed`);

  if (failed > 0) {
    process.exit(1);
  }
})().catch((error) => {
  console.error(error && error.stack ? error.stack : error);
  process.exit(1);
});
