const { performance } = require("node:perf_hooks");
const { MongoClient, ServerApiVersion } = require("mongodb");
const { map, chunk, isEmpty } = require("lodash");

const { MONGO_USER, MONGO_PASS, MONGO_ADDR } = process.env;
const mongoUri = getMongoUri(MONGO_USER, MONGO_PASS, MONGO_ADDR);

const JOB_CHUNK_SIZE = 50;
const CYCLE_TARGET_TIME_MS = 4.75 * 60 * 1000;

class JobManager {
  constructor() {
    this.client = new MongoClient(mongoUri, {
      serverApi: { version: ServerApiVersion.v1, strict: true },
    });

    this.db = this.client.db("dev");
    this.camerasCollection = this.db.collection("cameras");
    this.countsCollection = this.db.collection("counts");

    this.cameraChunks = [];
    this.numCameras = 0;
    this.jobIndex = 0;
    this.jobStartTime = 0;
    this.numCamerasCompleted = 0;
    this.cycleStartTime = 0;
  }

  async setup() {
    console.log({ JOB_CHUNK_SIZE });

    const cameras = await this.camerasCollection
      .find()
      .project({ urls: 1 })
      .toArray()
      .then((cameras) => map(cameras, ({ _id, urls }) => ({ urls, _id: _id.toString() })));

    if (isEmpty(cameras)) {
      throw new Error("No cameras fetched");
    }

    console.log(`Fetched ${cameras.length} cameras`);

    const cameraCounts = map(cameras, ({ urls }) => urls.length);
    if (Math.min(...cameraCounts) === 0) {
      throw new Error("At least one camera contains no urls");
    }

    this.cameraChunks = chunk(cameras, JOB_CHUNK_SIZE);
    this.numCameras = cameras.length;
  }

  getNextJobs() {
    if (this.numCamerasCompleted % this.numCameras === 0) {
      this.cycleStartTime = performance.now();
    }

    this.jobStartTime = performance.now();

    return this.cameraChunks[this.jobIndex];
  }

  async completeJobs(completedJobs) {
    if (completedJobs.length !== this.cameraChunks[this.jobIndex].length) {
      throw new Error("Invalid number of completed jobs");
    }

    await this.countsCollection.insertMany(completedJobs);

    this.jobIndex = (this.jobIndex + 1) % this.cameraChunks.length;
    this.numCamerasCompleted += completedJobs.length;

    const timeTaken = performance.now() - this.jobStartTime;
    const percentDone = completedJobs.length / this.numCameras;
    const sleepTime = CYCLE_TARGET_TIME_MS * percentDone - timeTaken;
    await sleep(sleepTime);

    if (this.numCamerasCompleted % this.numCameras === 0) {
      const cycleTimeTakenMin = ~~(performance.now() - this.cycleStartTime) / 1000 / 60;
      console.log(`Cycle took ${cycleTimeTakenMin} minutes`);
    }
  }

  async close() {
    await this.client.close();
  }
}

function getMongoUri(user, pass, addr) {
  return `mongodb+srv://${user}:${pass}@${addr}/?retryWrites=true&w=majority`;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

module.exports = { JobManager };
