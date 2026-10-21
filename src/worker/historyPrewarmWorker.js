const redisClient = require("../config/redis");
const logger = require("../utils/logger");
const collectionsRepository = require("../repositories/collections/collections.factory");
const weatherDataRepository = require("../repositories/weather_data/weatherData.factory");
const cassandra = require("cassandra-driver");

// Keep the dashboard's default view fast: refresh the "latest" timestamp and the
// last-24h aggregate for the focused device on a fixed interval, so user
// requests are served from Redis instead of scanning a large Scylla partition.
const PREWARM_INTERVAL_MS = 5 * 60 * 1000;
const PREWARM_TTL_SECONDS = 600;
const QUERY_TIMEOUT_MS = 30000;

// DEPLOY TNTF UGM
const PREWARM_DEVICE_ID =
  process.env.PREWARM_DEVICE_ID || "91e2e000-cb17-494c-8a45-c6182b2a89ac";

let isRunning = false;

const withTimeout = (promise, ms) =>
  new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("prewarm query timed out")), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (err) => {
        clearTimeout(timer);
        reject(err);
      }
    );
  });

async function prewarmDevice(deviceId) {
  const tableName = `records_${deviceId.replace(/-/g, "")}`;
  const collectionId = cassandra.types.Uuid.fromString(deviceId);

  const latest = await withTimeout(
    weatherDataRepository.getLatestTimestamp({ tableName, collectionId }),
    QUERY_TIMEOUT_MS
  );

  await redisClient.set(
    `latest:${deviceId}`,
    latest ?? "__none__",
    "EX",
    PREWARM_TTL_SECONDS
  );

  if (!latest) return;

  const schemaFields = await collectionsRepository.getSchemaFieldsByDeviceId(
    deviceId
  );
  if (!schemaFields) return;

  const endTime = new Date(latest);
  const startTime = new Date(endTime.getTime() - 24 * 60 * 60 * 1000);

  const data = await withTimeout(
    weatherDataRepository.getMinuteAverages({
      startTime,
      endTime,
      timezone: "Asia/Jakarta",
      tableName,
      collectionId,
      fields: Object.keys(schemaFields),
    }),
    QUERY_TIMEOUT_MS
  );

  await redisClient.set(
    `minute24:${deviceId}`,
    JSON.stringify(data),
    "EX",
    PREWARM_TTL_SECONDS
  );
}

async function prewarm() {
  if (isRunning) {
    logger.info("History prewarm still running — skipping this cycle.");
    return;
  }

  isRunning = true;
  try {
    await prewarmDevice(PREWARM_DEVICE_ID);
    logger.info(`History prewarm completed for ${PREWARM_DEVICE_ID}.`);
  } catch (err) {
    logger.warn(`History prewarm failed: ${err.message}`);
  } finally {
    isRunning = false;
  }
}

setInterval(prewarm, PREWARM_INTERVAL_MS);
