const uuid = require('uuid/v4');
const timeSpan = require('time-span');
const prettyMs = require('pretty-ms');

const AWS = require('aws-sdk');
const kinesis = new AWS.Kinesis({ region: 'us-west-2' });
const Promise = require('bluebird');

async function pushKinesis(items) {
  const ns = require('./dist/index');
  const parser = new ns.KinesisBridgeEnvelopeParser();

  if (!items || items.length <= 0) {
    return items;
  }

  let params = {
    Records: [],
    StreamName: 'wifire-dev'
  };

  for (let i = 0; i < items.length; i++) {
    let json = {
      headerVersion: 1,
      timeUUID: '',
      sourceNameLength: 0,
      sourceName: '',
      sourcePropertiesLength: 0,
      sourceProperties: {},
      targetNameLength: 0,
      targetName: '',
      targetPropertiesLength: 0,
      targetProperties: {
        SchemaVersion: 1,
        EntityType: 'topic',
        Owner: {
          Id: '',
          Properties: {
            SchemaVersion: 1,
            EntityType: 'device',
            AccountId: '',
            TemplateId: '',
            OrganizationIds: ['e24bf37c-353a-4c48-8fe6-46b6bd739e30']
          }
        }
      },
      contentLength: 17,
      contentBody: ''
    };

    const element = items[i];
    let id = uuid();
    json.timeUUID = id;
    json.sourceName = id;
    json.targetName = id;
    json.targetProperties.Owner.Id = id;
    json.targetProperties.Owner.Properties.AccountId = id;
    json.targetProperties.Owner.Properties.TemplateId = id;

    json.contentBody = JSON.parse(element);
    let enc = parser.encode(json);

    params.Records.push({
      Data: enc,
      PartitionKey: id
    });
  }

  let ret = kinesis.putRecords(params).promise();
  return ret;
}

let ts;
const lineByLine = require('n-readlines');
const rl = new lineByLine('./append.txt');

let line;
let failed = 0;
let totalPushed = 0;
let batch = [];
let tasks = [];

console.log('Load From Disk');

while ((line = rl.next())) {
  if (batch.length === 500) {
    tasks.push(batch.slice());
    batch = [];
  }

  if (line.length > 0) {
    batch.push(line.toString());
  }
}

tasks.push(batch.slice());
ts = timeSpan();
let tasksProcessedCount = 0;
let intervalTs = timeSpan();
let intervalCount = 0;

async function processLines(num, max) {
  return await Promise.map(
    tasks,
    task => {
      if (tasksProcessedCount % 100 === 0) {
        intervalCount = 0;
        intervalTs = timeSpan();
      }

      return pushKinesis(task).then(kResult => {
        failed += kResult.FailedRecordCount;
        totalPushed += task.length;
        intervalCount += task.length;
        console.log(
          `Processed: ${++tasksProcessedCount}/${tasks.length * max} Time: ${prettyMs(ts.rounded(), {
            keepDecimalsOnWholeSeconds: true
          })} Records: ${totalPushed} Rate: ${(intervalCount / intervalTs.seconds()).toFixed(0)} per/sec Failed: ${failed}`
        );
        if (kResult.FailedRecordCount > 0) {
          let failed = kResult.Records.find(rr => {
            return rr.ErrorMessage && rr.ErrorMessage.length > 0;
          });

          console.log(JSON.stringify(failed, null, 2));

          setTimeout(function() {
            console.log('Waiting');
          }, 5000);
        }

        return kResult;
      });
    },
    { concurrency: 10 }
  )
    .catch(err => {
      console.error(err);
    })
    .finally(() => {
      console.log(`Iteration ${num} of ${max}`);
    });
}

Promise.each([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], async num => {
  return await processLines(num, 10);
});
