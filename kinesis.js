const uuid = require("uuid/v4");
const timeSpan = require("time-span");
const prettyMs = require("pretty-ms");

const AWS = require("aws-sdk");
const kinesis = new AWS.Kinesis({ region: "us-west-2" });
const Promise = require("bluebird");

async function pushKinesis(items) {
  const ns = require("./dist/index");
  const parser = new ns.KinesisBridgeEnvelopeParser();

  if (!items || items.length <= 0) {
    return items;
  }

  let params = {
    Records: [],
    StreamName: "wifire-dev"
  };

  for (let i = 0; i < items.length; i++) {
    let json = {
      headerVersion: 1,
      timeUUID: "",
      sourceNameLength: 0,
      sourceName: "",
      sourcePropertiesLength: 0,
      sourceProperties: {},
      targetNameLength: 0,
      targetName: "",
      targetPropertiesLength: 0,
      targetProperties: {
        SchemaVersion: 1,
        EntityType: "topic",
        Owner: {
          Id: "",
          Properties: {
            SchemaVersion: 1,
            EntityType: "device",
            AccountId: "",
            TemplateId: "",
            OrganizationIds: ["e24bf37c-353a-4c48-8fe6-46b6bd739e30"]
          }
        }
      },
      contentLength: 17,
      contentBody: ""
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
const lineByLine = require("n-readlines");
const rl = new lineByLine("./append.txt");

let line;
let failed = 0;
let totalPushed = 0;
let batch = [];
let tasks = [];

console.log("Load From Disk");

let lasttime = "";
let curtime = "";
while ((line = rl.next())) {
  if (line.length > 0) {
    let str = line.toString();
    let obj = JSON.parse(str);
    curtime = obj.bt;

    if (batch.length === 500 || lasttime !== curtime) {
      lasttime = curtime;
      tasks.push(batch.slice());
      batch = [];
    }
    batch.push(str);
  }
}

tasks.push(batch.slice());
ts = timeSpan();
let tasksProcessedCount = 0;
let intervalTs = timeSpan();
let intervalCount = 0;

function chunk(arr, chunkSize) {
  // https://stackoverflow.com/a/24782004/205705
  let R = [];
  for (let i = 0, len = arr.length; i < len; i += chunkSize) {
    R.push(arr.slice(i, i + chunkSize));
  }
  return R;
}

let totalCount = tasks.length;

tasks = chunk(tasks, 30);

async function processLines(num, max) {
  return await Promise.each(tasks, task => {
    let len = task.reduce((acc, cur) => {
      return acc + cur.length;
    }, 0);

    totalPushed += len;
    intervalCount += len;
    tasksProcessedCount += task.length;

    if (tasksProcessedCount > 100) {
      console.log(
        `Processed: ${tasksProcessedCount}/${totalCount * max} Time: ${prettyMs(ts.rounded(), {
          keepDecimalsOnWholeSeconds: true
        })} Records: ${totalPushed} Rate: ${(intervalCount / intervalTs.seconds()).toFixed(0)} per/sec Failed: ${failed}`
      );
    }

    if (tasksProcessedCount % 300 === 0) {
      intervalCount = 0;
      intervalTs = timeSpan();
    }

    return Promise.map(task, async ea => {
      if (ea.length === 0) {
        return;
      }
      const kResult = await pushKinesis(ea);
      failed += kResult.FailedRecordCount;
      if (kResult.FailedRecordCount > 0) {
        let failed = kResult.Records.find(rr => {
          return rr.ErrorMessage && rr.ErrorMessage.length > 0;
        });
        console.log(JSON.stringify(failed, null, 2));
        setTimeout(function() {
          console.log("Waiting");
        }, 5000);
      }
      return kResult;
    });
  })
    .catch(err => {
      console.error(err);
    })
    .finally(() => {
      console.log(`Iteration ${num} of ${max} complete`);
    });
}

Promise.each([1], async num => {
  return await processLines(num, 1);
});
