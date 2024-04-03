const { performance } = require("node:perf_hooks");
const { MongoClient, ServerApiVersion } = require("mongodb");
const _ = require("lodash");

const { MONGO_USER, MONGO_PASS, MONGO_ADDR } = process.env;
const mongoUri = getMongoUri(MONGO_USER, MONGO_PASS, MONGO_ADDR);

const JOB_SIZE = 25;

class JobManager {
  constructor() {
    this.client = new MongoClient(mongoUri, {
      serverApi: { version: ServerApiVersion.v1, strict: true },
    });

    this.db = this.client.db("dev");
    this.countsCollection = this.db.collection("counts");
    this.snapshotsCollection = this.db.collection("snapshots");
    this.topCamerasCollection = this.db.collection("top_cameras");

    this.jobChunks = [];
    this.nextJobChunkIndex = 0;
    this.cycleStartTime;
  }

  async setup() {
    const [topCameras] = await this.topCamerasCollection.find().sort({ timestamp: -1 }).limit(1).toArray();
    if (_.isEmpty(topCameras)) {
      throw new Error("Failed to fetch topCameras");
    }

    const [snapshot] = await this.snapshotsCollection.find().sort({ timestamp: -1 }).limit(1).toArray();
    if (_.isEmpty(snapshot)) {
      throw new Error("Failed to fetch snapshot");
    }

    const { top_cameras, timestamp: topCamerasTimestamp } = topCameras;
    console.log(`topCameras timestamp: ${topCamerasTimestamp.toISOString()}`);

    const { data: snapshotData, timestamp: snapshotTimestamp } = snapshot;
    console.log(`snapshot timestamp: ${snapshotTimestamp.toISOString()}`);

    const { CA, USA } = top_cameras;
    const numCamerasCA = _.reduce(_.toPairs(CA), (pv, [_, cameraData]) => pv + cameraData.length, 0);
    const numCamerasUS = _.reduce(_.toPairs(USA), (pv, [_, cameraData]) => pv + cameraData.length, 0);
    console.log(`topCameras contains ${numCamerasCA} CA cameras and ${numCamerasUS} US cameras (but more URLs)`);

    const jobs = createJobs(top_cameras, snapshotData);
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

function createJobs(top_cameras, snapshotData) {
  const jobs = [];
  for (const [country, countryData] of _.toPairs(top_cameras)) {
    for (const [locality, cameraData] of _.toPairs(countryData)) {
      for (const { internal_id } of cameraData) {
        const camera = _.find(snapshotData[country][locality]["cameraData"], (v) => v["internalId"] === internal_id);
        if (_.isNil(camera)) {
          throw new Error("Failed to find topCamera in snapshot");
        }
        jobs.push({ country, locality, camera });
      }
    }
  }
  return _.shuffle(jobs);
}

module.exports = { JobManager };
