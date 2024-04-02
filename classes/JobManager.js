const { performance } = require("node:perf_hooks");
const { MongoClient, ServerApiVersion } = require("mongodb");
const _ = require("lodash");

const { MONGO_USER, MONGO_PASS, MONGO_ADDR } = process.env;
const mongoUri = getMongoUri(MONGO_USER, MONGO_PASS, MONGO_ADDR);

const JOB_SIZE = 50;

class JobManager {
  constructor() {
    this.client = new MongoClient(mongoUri, {
      serverApi: { version: ServerApiVersion.v1, strict: true },
    });

    this.db = this.client.db("dev");
    this.snapshotsCollection = this.db.collection("snapshots");
    this.countsCollection = this.db.collection("counts");

    this.jobChunks = [];
    this.nextJobChunkIndex = 0;
    this.cycleStartTime;
  }

  async setup() {
    const [snapshot] = await this.snapshotsCollection.find().sort({ timestamp: -1 }).limit(1).toArray();

    if (_.isEmpty(snapshot)) {
      throw new Error("Failed to fetch snapshot");
    }

    const { data, timestamp } = snapshot;
    console.log(`Snapshot timestamp: ${timestamp.toISOString()}`);

    const { CA, USA } = data;
    const numCamerasCA = _.reduce(_.toPairs(CA), (pv, [_, { cameraData }]) => pv + cameraData.length, 0);
    const numCamerasUSA = _.reduce(_.toPairs(USA), (pv, [_, { cameraData }]) => pv + cameraData.length, 0);
    console.log(`Snapshot contains ${numCamerasCA} CA cameras and ${numCamerasUSA} USA cameras (but more URLs)`);

    const jobs = createJobs(data);
    this.jobChunks = _.chunk(jobs, JOB_SIZE);
    console.log(`Created ${jobs.length} jobs (${this.jobChunks.length} chunks of ${JOB_SIZE})`);
  }

  getNextJob() {
    if (this.nextJobChunkIndex === 0) {
      this.cycleStartTime = performance.now();
    }

    return this.jobChunks[this.nextJobChunkIndex];
  }

  async completeJob(completedJob) {
    _.forEach(completedJob, ({ country, locality, internalId, count, time }) => {
      if (_.isNil(country) || _.isNil(locality) || _.isNil(internalId) || _.isNil(count) || _.isNil(time)) {
        throw new Error("Job contains insufficient data");
      }
    });

    await this.countsCollection.insertMany(completedJob);

    const percentDone = Math.round(((this.nextJobChunkIndex + 1) / this.jobChunks.length) * 10000) / 100;
    this.nextJobChunkIndex = (this.nextJobChunkIndex + 1) % this.jobChunks.length;
    console.log(`Percent done: ${percentDone}`);

    if (this.nextJobChunkIndex === 0) {
      const cycleTimeTakenMin = ~~(performance.now() - this.cycleStartTime) / 1000 / 60;
      console.log(`Full job cycle took ${cycleTimeTakenMin}m`);
    }
  }

  async close() {
    await this.client.close();
  }
}

function getMongoUri(user, pass, addr) {
  return `mongodb+srv://${user}:${pass}@${addr}/?retryWrites=true&w=majority`;
}

function createJobs(data) {
  const jobs = [];
  for (const [country, countryData] of _.toPairs(data)) {
    for (const [locality, { headers, cameraData }] of _.toPairs(countryData)) {
      for (const camera of cameraData) {
        jobs.push({ country, locality, camera, headers });
      }
    }
  }
  return _.shuffle(jobs);
}

module.exports = { JobManager };
