const cassandra = require("cassandra-driver");
const redis = require("../../config/redis");
const weatherDataRepository = require("../../repositories/weather_data/weatherData.factory");
const CustomError = require("../../helpers/customError");
const collectionsRepository = require("../../repositories/collections/collections.factory");
const { getLatest: getLatestFromStream } = require("../../helpers/latestStore");

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// Short-lived cache so a repeatedly-loaded dashboard does not trigger a
// server-side max() over the device partition on every request.
const LATEST_CACHE_TTL_SECONDS = 45;

// The Scylla max() fallback can be slow on a large partition; cap it so a slow
// cluster can never hang the request.
const LATEST_FALLBACK_TIMEOUT_MS = 2500;

const withTimeout = (promise, ms) =>
  new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("latest lookup timed out")), ms);
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

/**
 * Validate deviceId and derive the Scylla identifiers.
 * Returns a 400 (instead of a 500 "Invalid table name") for malformed ids.
 */
const resolveDevice = (deviceId) => {
  if (!deviceId) {
    throw new CustomError({ message: "deviceId is required", statusCode: 400 });
  }
  if (!UUID_RE.test(deviceId)) {
    throw new CustomError({
      message: "deviceId must be a valid UUID",
      statusCode: 400,
    });
  }
  return {
    deviceId,
    collectionId: cassandra.types.Uuid.fromString(deviceId),
    tableName: `records_${deviceId.replace(/-/g, "")}`,
  };
};

const getWeatherDataAverages = async (req) => {
  let {
    startTime,
    endTime,
    timezone,
    interval = "minute",
    deviceId,
  } = req.query;

  const { collectionId, tableName } = resolveDevice(deviceId);

  if (!startTime || !endTime) {
    throw new CustomError({
      message: "Both startTime and endTime are required",
      statusCode: 400,
    });
  }

  const schemaFields = await collectionsRepository.getSchemaFieldsByDeviceId(
    deviceId
  );
  if (!schemaFields) {
    throw new CustomError({
      message: "Device schema not found",
      statusCode: 404,
    });
  }

  const fields = Object.keys(schemaFields);

  startTime = new Date(startTime);
  endTime = new Date(endTime);

  if (isNaN(startTime.getTime()) || isNaN(endTime.getTime())) {
    throw new CustomError({
      message: "Invalid date format. Use ISO format",
      statusCode: 400,
    });
  }

  if (startTime >= endTime) {
    throw new CustomError({
      message: "startTime must be before endTime",
      statusCode: 400,
    });
  }

  const maxRangeDays =
    parseInt(process.env.WEATHER_DATA_MAX_RANGE_DAYS, 10) || 31;
  const maxRange = maxRangeDays * 24 * 60 * 60 * 1000;
  if (endTime - startTime > maxRange) {
    throw new CustomError({
      message: `Time range cannot exceed ${maxRangeDays} days`,
      statusCode: 400,
    });
  }

  // Fetch data from the device's Scylla partition
  let data;
  switch (interval) {
    case "minute":
      data = await weatherDataRepository.getMinuteAverages({
        startTime,
        endTime,
        timezone: timezone || "UTC",
        tableName,
        collectionId,
        fields,
      });
      break;
    case "raw":
      data = await weatherDataRepository.getDataInTimeRange({
        startTime,
        endTime,
        tableName,
        collectionId,
      });
      break;
    default:
      throw new CustomError({
        message: "Invalid interval. Use 'minute' or 'raw'",
        statusCode: 400,
      });
  }

  return {
    deviceId,
    startTime: startTime.toISOString(),
    endTime: endTime.toISOString(),
    interval,
    timezone: timezone || "UTC",
    data,
  };
};

/**
 * Get the most recent timestamp available for a device.
 * Used by the dashboard to auto-select a time range that actually has data.
 */
const getLatestWeatherTime = async (req) => {
  const { deviceId, collectionId, tableName } = resolveDevice(req.query.deviceId);

  const cacheKey = `latest:${deviceId}`;
  const cached = await redis.get(cacheKey);
  if (cached !== null && cached !== undefined) {
    return { deviceId, latest: cached === "__none__" ? null : cached };
  }

  // Preferred: the newest timestamp recorded from the MQTT stream (O(1)).
  let latest = await getLatestFromStream(deviceId);

  // Fallback for devices that have not been heard from recently: ask Scylla,
  // but cap the wait and degrade to null instead of failing the request.
  if (!latest) {
    try {
      latest = await withTimeout(
        weatherDataRepository.getLatestTimestamp({ tableName, collectionId }),
        LATEST_FALLBACK_TIMEOUT_MS
      );
    } catch (err) {
      latest = null;
    }
  }

  await redis.set(cacheKey, latest ?? "__none__", "EX", LATEST_CACHE_TTL_SECONDS);

  return { deviceId, latest };
};

module.exports = {
  getWeatherDataAverages,
  getLatestWeatherTime,
};