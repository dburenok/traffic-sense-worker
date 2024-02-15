const Axios = require("axios");
const FormData = require("form-data");
const { isNil, map, forEach, isEmpty, some } = require("lodash");
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
    const jobs = jobManager.getNextJobs(); // { _id, urls }[]

    const promises = map(jobs, ({ urls, _id }) =>
      fetchImages(urls, _id).then((imageDataDto) => {
        const jobSuccessful = !some(imageDataDto, ({ fetchSuccess }) => !fetchSuccess);
        if (!jobSuccessful) {
          return {
            intersectionId: _id,
            time: new Date(),
            count: 0,
            fetchSuccess: false,
          };
        }

        const form = new FormData();
        forEach(imageDataDto, ({ imageData, imageName }) => form.append("images", imageData, imageName));

        return Axios.post(INFERENCE_ENDPOINT, form).then((res) => ({
          intersectionId: _id,
          time: new Date(),
          count: parseInt(res.data["vehicle_count"]),
          fetchSuccess: true,
        }));
      }),
    );

    const completedJobs = await Promise.all(promises);
    await jobManager.completeJobs(completedJobs);
  } catch (e) {
    console.error(e);
  }

  setTimeout(run);
}

jobManager.setup().then(() => begin());

async function fetchImages(urls, id) {
  const promises = map(urls, (url) =>
    Axios.get(url, { responseType: "arraybuffer" })
      .catch(() => ({ fetchSuccess: false }))
      .then(({ fetchSuccess, ...res }) =>
        isNil(fetchSuccess) && !isNil(res) && !isNil(res["data"])
          ? { url, fetchSuccess: true, res }
          : { url, fetchSuccess: false },
      ),
  );
  const responses = await Promise.all(promises);

  const failedResponses = responses.filter((r) => !r.fetchSuccess);
  if (!isEmpty(failedResponses)) {
    console.log(`Fetch failure in ${id}, marking as unsuccessful`);
  }

  return map(responses, ({ url, fetchSuccess, res }, i) => {
    return fetchSuccess === true
      ? {
          fetchSuccess,
          imageData: Buffer.from(res["data"]),
          imageName: `${id}${ID_INDEX_SEPARATOR}${i + 1}.${res["headers"]["content-type"].split("/")[1]}`,
        }
      : {
          fetchSuccess,
        };
  });
}

async function establishApiConnection() {
  return Axios.get(HEALTH_ENDPOINT).then(
    (res) => !isNil(res.data["message"]) && res.data["message"].includes("API is up"),
  );
}
