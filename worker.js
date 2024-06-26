require("dotenv").config();

const Axios = require("axios");
const FormData = require("form-data");
const _ = require("lodash");
const { JobManager } = require("./classes/JobManager");

const { APP_ENV } = process.env;
const jobManager = new JobManager();

const HEALTH_ENDPOINT = APP_ENV === "dev" ? "http://0.0.0.0:8080/api/health/" : "http://api:8080/api/health/";
const INFERENCE_ENDPOINT = APP_ENV === "dev" ? "http://0.0.0.0:8080/api/inference/" : "http://api:8080/api/inference/";
const ID_INDEX_SEPARATOR = "__";

async function begin() {
  try {
    await establishApiConnection();
    console.log(`Pinged inference API at ${HEALTH_ENDPOINT}`);
    run();
  } catch (e) {
    console.log("Failed to establish connection with inference API. Exiting...");
    await jobManager.close();
  }
}

async function run() {
  try {
    const job = jobManager.getNextJob();

    const promises = _.map(job, async ({ country, locality, camera }) => {
      const { imageUrls, internalId } = camera;
      const time = new Date();
      const imageDataDto = await fetchImages(imageUrls, internalId);
      const jobSuccessful = !_.some(imageDataDto, ({ fetchSuccess }) => !fetchSuccess);
      if (!jobSuccessful) {
        console.log(`Fetch failure in ${imageUrls}`);
        return { country, locality, internalId, count: -1, time };
      }

      const form = new FormData();
      _.forEach(imageDataDto, ({ imageData, imageName }) => form.append("images", imageData, imageName));
      const apiResponse = await Axios.post(INFERENCE_ENDPOINT, form);
      const count = parseInt(apiResponse.data["vehicle_count"]);
      if (count === -1) {
        console.log(`Inference failure in ${imageUrls}`);
      }
      return { country, locality, internalId, count, time };
    });

    const completedJob = await Promise.all(promises);

    await jobManager.completeJob(completedJob);
  } catch (e) {
    console.error(e);
  }

  setTimeout(run, 1000);
}

jobManager.setup().then(() => begin());

setInterval(() => {
  const memoryData = process.memoryUsage();
  const memoryUsage = {
    rss: `${formatMemoryUsage(memoryData.rss)} -> Resident Set Size - total memory allocated for process execution`,
    heapTotal: `${formatMemoryUsage(memoryData.heapTotal)} -> total size of allocated heap`,
    heapUsed: `${formatMemoryUsage(memoryData.heapUsed)} -> actual memory used during execution`,
    external: `${formatMemoryUsage(memoryData.external)} -> V8 external memory`,
  };
  console.log(memoryUsage);
}, 15 * 60 * 1000);

async function fetchImages(urls, internalId) {
  const promises = _.map(urls, (url) =>
    Axios.get(url, { responseType: "arraybuffer" })
      .catch(() => ({ fetchSuccess: false }))
      .then(({ fetchSuccess, ...res }) =>
        _.isNil(fetchSuccess) && !_.isNil(res) && !_.isNil(res["data"])
          ? { fetchSuccess: true, res }
          : { fetchSuccess: false }
      )
  );
  const responses = await Promise.all(promises);
  return _.map(responses, ({ fetchSuccess, res }, i) => {
    return fetchSuccess
      ? {
          fetchSuccess,
          imageData: Buffer.from(res["data"]),
          imageName: `${internalId}${ID_INDEX_SEPARATOR}${i + 1}`,
        }
      : { fetchSuccess };
  });
}

async function establishApiConnection() {
  return Axios.get(HEALTH_ENDPOINT).then(({ data }) => !_.isNil(data) && !_.isNil(data.message));
}

function formatMemoryUsage(data) {
  return `${Math.round((data / 1024 / 1024) * 100) / 100} MB`;
}
