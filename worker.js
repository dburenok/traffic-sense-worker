const Axios = require("axios");
const FormData = require("form-data");
const { last, isNil, sum, values } = require("lodash");
const { JobManager } = require("./classes/JobManager");

const { MONGO_USER, MONGO_PASS, MONGO_ADDR } = process.env;
const mongoUri = getMongoUri(MONGO_USER, MONGO_PASS, MONGO_ADDR);
const jobManager = new JobManager(mongoUri);

const HEALTH_ENDPOINT = "http://api:14641/api/";
const INFERENCE_ENDPOINT = "http://api:14641/api/inference/";

async function main() {
  console.log(`${prefix()} Pinging inference API at ${HEALTH_ENDPOINT}`);
  for (let i = 0; i < 5; i++) {
    try {
      await inferenceApiConnectionEstablished();
      console.log(`${prefix()} Connection established!`);
      run();
      return;
    } catch (e) {
      console.log(`${prefix()} Failed to establish connection with inference API. Trying again...`);
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
  console.log(`${prefix()} Failed to establish connection with inference API. Exiting...`);
  process.exit();
}

process.on("SIGINT", () => {
  process.exit();
});

async function run() {
  try {
    const { intersection, imageUrls } = await jobManager.getNextJob();
    const images = await fetchImages(intersection, imageUrls);
    const timestamp = new Date();

    const form = new FormData();
    images.forEach(({ imageData, intersection }) => form.append("images", imageData, intersection));

    const res = await Axios.post(INFERENCE_ENDPOINT, form, {
      headers: {
        Accept: "application/json",
        "Content-Type": "multipart/form-data",
      },
    }).then((res) => res.data);

    const vehicleCount = {
      timestamp,
      intersection,
      vehicleCount: getVehicleCount(res),
    };
    await jobManager.completeJob(vehicleCount);
  } catch (e) {
    console.error(e);
  }

  setTimeout(run);
}

main();

async function fetchImages(intersection, imageUrls) {
  const promises = imageUrls.map(async (url) => ({
    res: await Axios.get(url, { responseType: "arraybuffer" }).catch(() => null),
    url,
  }));
  const responses = await Promise.all(promises);
  const nonNullResponses = responses.filter(({ res }) => res !== null);

  return nonNullResponses.map(({ res, url }, i) => ({
    imageData: Buffer.from(res.data),
    intersection: `${intersection}-${i + 1}.${last(url.split("."))}`,
  }));
}

async function inferenceApiConnectionEstablished() {
  return await Axios.get(HEALTH_ENDPOINT).then(
    (res) => !isNil(res.data["message"]) && res.data["message"].includes("API is up")
  );
}

function getVehicleCount(res) {
  return sum(values(res["vehicle_counts"]));
}

function getMongoUri(user, pass, addr) {
  return `mongodb+srv://${user}:${pass}@${addr}/?retryWrites=true&w=majority`;
}

function prefix() {
  return `[WRK ${new Date().toLocaleTimeString()}]`;
}
