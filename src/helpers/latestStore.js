const redis = require("../config/redis");

// Redis hash storing the last sensor timestamp seen per device, filled from the
// MQTT stream. Reading one field is O(1) and avoids scanning the (unsorted,
// multi-million row) Scylla partition for a max() on every dashboard load.
const LATEST_HASH_KEY = "mw:latest";

const recordLatest = async (deviceId, timestamp) => {
  if (!deviceId) return;
  const value = (timestamp instanceof Date ? timestamp : new Date(timestamp))
    .toISOString();
  await redis.hset(LATEST_HASH_KEY, deviceId, value);
};

const getLatest = async (deviceId) => {
  if (!deviceId) return null;
  return redis.hget(LATEST_HASH_KEY, deviceId);
};

module.exports = { recordLatest, getLatest };
