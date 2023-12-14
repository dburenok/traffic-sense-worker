const Axios = require("axios");
const FormData = require("form-data");
const { isNil, map, forEach, isEmpty, flatten, toPairs } = require("lodash");
const { JobManager } = require("./classes/JobManager");

const { APP_ENV } = process.env;
const jobManager = new JobManager();

const HEALTH_ENDPOINT = APP_ENV === "dev" ? "http://0.0.0.0:8080/api/health/" : "http://api:8080/api/health/";
const INFERENCE_ENDPOINT = APP_ENV === "dev" ? "http://0.0.0.0:8080/api/inference/" : "http://api:8080/api/inference/";
const ID_INDEX_SEPARATOR = "__";

async function begin() {
  try {
    await establishApiConnection();
    console.log(`Pinged inference API at ${HEALTH_ENDPOINT}!`);
    run();
  } catch (e) {
    console.log("Failed to establish connection with inference API. Exiting...");
    await jobManager.close();
  }
}

async function run() {
  try {
    const jobs = jobManager.getNextJobs(); // { _id, urls }[]
    const jobsWithImages = await Promise.all(
      map(jobs, ({ _id, urls }) => fetchImages(urls, _id).then((jobImages) => ({ jobImages, _id }))),
    );

    if (isEmpty(jobsWithImages)) {
      throw new Error(`Failed to fetch job images`);
    }

    const timestamp = new Date();
    const form = new FormData();
    const jobImages = flatten(map(jobsWithImages, "jobImages"));
    forEach(jobImages, ({ imageData, imageName }) => form.append("images", imageData, imageName));

    const { vehicle_counts } = await Axios.post(INFERENCE_ENDPOINT, form).then((res) => res.data);
    const completedJobs = map(toPairs(vehicle_counts), ([imageName, count]) => ({
      intersectionId: imageName.split(ID_INDEX_SEPARATOR)[0],
      time: timestamp,
      count,
    }));

    await jobManager.completeJobs(completedJobs);
  } catch (e) {
    console.error(e);
  }

  setTimeout(run);
}

jobManager.setup().then(() => begin());

async function fetchImages(urls, id) {
  const promises = map(urls, (url) => Axios.get(url, { responseType: "arraybuffer" }).catch(() => null));
  const responses = await Promise.all(promises);
  const nonNullResponses = responses.filter((res) => !isNil(res));

  if (isEmpty(nonNullResponses)) {
    console.log(`Fetched no images for`, urls);
  }

  return nonNullResponses.map((res, i) => ({
    imageData: Buffer.from(res.data),
    imageName: `${id}${ID_INDEX_SEPARATOR}${i + 1}.${res["headers"]["content-type"].split("/")[1]}`,
  }));
}

async function establishApiConnection() {
  return Axios.get(HEALTH_ENDPOINT).then(
    (res) => !isNil(res.data["message"]) && res.data["message"].includes("API is up"),
  );
}
