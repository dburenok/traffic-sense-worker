const { MongoClient, ServerApiVersion } = require("mongodb");
const fs = require("fs");
const { head, keyBy, sum } = require("lodash");
const { performance } = require("perf_hooks");

class JobManager {
  constructor(mongoUri) {
    this.cameraData = loadCameraData();
    this.client = new MongoClient(mongoUri, {
      serverApi: { version: ServerApiVersion.v1, strict: true },
    });
    this.db = this.client.db("dev");
    this.vehicleCounts = this.db.collection("vehiclecounts");
    this.jobLog = this.db.collection("joblog");

    this.jobStartTime = null;
    this.numJobsCompleted = 0;
    this.processingTimes = [];
  }

  async getNextJob() {
    this.jobStartTime = performance.now();
    const intersection = await this.jobLog
      .find()
      .toArray()
      .then(head)
      .then(({ nextJobIndex, intersectionNames }) => intersectionNames[nextJobIndex]);

    return {
      intersection,
      imageUrls: this.cameraData[intersection]["imageUrls"],
    };
  }

  async completeJob(vehicleCounts) {
    const { intersection } = vehicleCounts;
    const session = this.client.startSession();
    try {
      session.startTransaction();
      await this.vehicleCounts.insertOne(vehicleCounts, { session });
      const { currentJobIndex, numIntersections } = await this.jobLog
        .find()
        .toArray()
        .then(head)
        .then(({ nextJobIndex, intersectionNames }) => ({
          currentJobIndex: nextJobIndex,
          numIntersections: intersectionNames.length,
        }));
      const nextJobIndex = (currentJobIndex + 1) % numIntersections;
      await this.jobLog.updateOne({}, { $set: { nextJobIndex } }, { session });
      await session.commitTransaction();
      this.numJobsCompleted++;
      const timeTakenMs = performance.now() - this.jobStartTime;
      this.processingTimes = [timeTakenMs, ...this.processingTimes.slice(0, 9)];
      console.log(`${prefix()} Job for "${intersection}" completed`);
      this.reportAvgProcessingTime();
    } catch (e) {
      console.error("An error occured in the transaction, performing rollback:", e);
      await session.abortTransaction();
    } finally {
      await session.endSession();
    }
  }

  reportAvgProcessingTime() {
    if (this.numJobsCompleted % 10 !== 0) {
      return;
    }
    const average = Math.round(sum(this.processingTimes) / this.processingTimes.length) / 1000;
    console.log(`${prefix()} ${this.numJobsCompleted} jobs completed, average job time is ${average}s`);
  }
}

function loadCameraData() {
  const jsonData = fs.readFileSync("./data/camera_data.json");
  const jsonParsed = JSON.parse(jsonData);

  return keyBy(jsonParsed, "name");
}

function prefix() {
  return `[MGR ${new Date().toLocaleTimeString()}]`;
}

module.exports = { JobManager };
