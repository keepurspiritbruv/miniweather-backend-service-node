const mqtt = require("mqtt");
const {
  MQTT_BROKER_URL,
  MQTT_HYPERBASE_TOPIC,
} = require("../config/mqttConfig");
const logger = require("../utils/logger");
const { pool } = require("../config/postgre");
const { publishMessage } = require("./publisher");
const { getHyperbaseStatus } = require("../socket/index");
const redisClient = require("../config/redis"); 
const { recordLatest } = require("../helpers/latestStore");

// Track active subscriptions
const activeSubscriptions = new Set();
const deviceConfigurations = new Map();
const lastProcessedMessageTime = new Map();

const mqttSubscriber = mqtt.connect(MQTT_BROKER_URL);

const project_id = process.env.HYPERBASE_PROJECT_ID;
const token_id = process.env.HYPERBASE_TOKEN_ID;

// Initialize MQTT connection and subscriptions
const initializeSubscriptions = async () => {
  try {
    // Fetch device ID and data_interval_seconds for active devices
    const result = await pool.query(
      "SELECT id, data_interval_seconds FROM onboarding_devices WHERE status = 'active'"
    );

    deviceConfigurations.clear();
    activeSubscriptions.clear();
    lastProcessedMessageTime.clear();

    for (const { id, data_interval_seconds } of result.rows) {
      deviceConfigurations.set(id, { data_interval_seconds });
      logger.info(
        `Device ${id} configured with data_interval_seconds: ${
          data_interval_seconds || "not set"
        }`
      );
      await subscribeToDevice(id);
    }
  } catch (err) {
    logger.error("Error initializing subscriptions:", err);
  }
};

// Subscribe to a device topic
const subscribeToDevice = async (deviceId) => {
  const topic = `/devices/${deviceId}`;

  return new Promise((resolve, reject) => {
    if (activeSubscriptions.has(topic)) {
      logger.info(`Already subscribed to ${topic}. Skipping.`);
      return resolve();
    }

    mqttSubscriber.subscribe(topic, (err) => {
      if (err) {
        logger.error(`Failed to subscribe to ${topic}: ${err.message}`);
        reject(err);
      } else {
        logger.info(`Subscribed to device topic ${topic}`);
        activeSubscriptions.add(topic);
        resolve();
      }
    });
  });
};

// Unsubscribe from a device topic
const unsubscribeFromDevice = async (deviceId) => {
  const topic = `/devices/${deviceId}`;

  return new Promise((resolve, reject) => {
    if (!activeSubscriptions.has(topic)) {
      logger.info(`Not subscribed to ${topic}. Skipping unsubscribe.`);
      return resolve();
    }

    mqttSubscriber.unsubscribe(topic, (err) => {
      if (err) {
        logger.error(`Failed to unsubscribe from ${topic}: ${err.message}`);
        reject(err);
      } else {
        logger.info(`Unsubscribed from device topic ${topic}`);
        activeSubscriptions.delete(topic);
        deviceConfigurations.delete(deviceId);
        lastProcessedMessageTime.delete(deviceId);
        resolve();
      }
    });
  });
};

const updateDeviceDataInterval = async (deviceId, newDataIntervalSeconds) => {
  try {
    deviceConfigurations.set(deviceId, {
      data_interval_seconds: newDataIntervalSeconds,
    });
    logger.info(
      `Successfully updated data interval for device ${deviceId} to ${
        newDataIntervalSeconds || "null"
      } seconds in in-memory cache.`
    );
    return true;
  } catch (err) {
    logger.error(
      `Error updating data interval in cache for device ${deviceId}:`,
      err
    );
    return false;
  }
};

mqttSubscriber.on("message", async (topic, message) => {
  try {
    const topicParts = topic.split("/");
    const deviceId = topicParts[2];

    const deviceConfig = deviceConfigurations.get(deviceId);
    const dataIntervalSeconds = deviceConfig
      ? deviceConfig.data_interval_seconds
      : null;
    const lastProcessedTime = lastProcessedMessageTime.get(deviceId) || 0; // Default to 0 if never processed
    const currentTime = Date.now(); // Current time in milliseconds

    // Check if the message should be throttled based on data_interval_seconds
    // If data_interval_seconds is null, undefined, or 0, always process the message.
    // Otherwise, process only if enough time has passed since the last processed message.
    if (
      dataIntervalSeconds === null ||
      dataIntervalSeconds === undefined ||
      dataIntervalSeconds <= 0 ||
      currentTime - lastProcessedTime >= dataIntervalSeconds * 1000
    ) {
      // Update the last processed time for this device
      lastProcessedMessageTime.set(deviceId, currentTime);

      if (dataIntervalSeconds !== null && dataIntervalSeconds !== undefined) {
        logger.info(
          `Device ${deviceId} processing message. Configured interval: ${dataIntervalSeconds} seconds.`
        );
      } else {
        logger.info(
          `Device ${deviceId} processing message. No data interval configured.`
        );
      }

      let parsedMessage;
      try {
        parsedMessage = JSON.parse(message.toString());
        // logger.debug("Parsed MQTT message:", parsedMessage);
      } catch (parseErr) {
        logger.warn(
          `Could not parse MQTT message from ${topic} as JSON. Content: ${message.toString()}`
        );
        // If it's not JSON, we'll just proceed with the raw message for Hyperbase/Socket.io
        parsedMessage = message.toString();
      }

      const collection_id = topic.split("/")[2]; // Assuming deviceId is used as collection_id

      // Remember the newest timestamp seen for this device so /weather-data/latest
      // can answer from Redis without the expensive Scylla max().
      try {
        const payloadTs =
          parsedMessage && typeof parsedMessage === "object"
            ? parsedMessage.timestamp || parsedMessage._updated_at
            : null;
        await recordLatest(deviceId, payloadTs || new Date());
      } catch (err) {
        logger.warn(`Failed to record latest timestamp for ${deviceId}: ${err.message}`);
      }

      if (getHyperbaseStatus()) {
        // Hyperbase is online
        publishMessage(
          MQTT_HYPERBASE_TOPIC,
          message,
          project_id,
          token_id,
          collection_id
        );
      } else {
        // Hyperbase is offline → save to Redis
        const cacheKey = `buffer:${deviceId}:${Date.now()}`;
        await redisClient.set(
          cacheKey,
          JSON.stringify({
            topic,
            deviceId,
            message: message.toString(),
            timestamp: new Date().toISOString(),
          })
        );
        logger.info(
          `Hyperbase offline — message buffered to Redis key ${cacheKey}`
        );
      }


      if (global.io) {
        // Emit the original message (or parsed, if you prefer)
        global.io.emit(topic, message.toString());
        // console.log("Emitted to socket.io:", topic, message.toString());
      }
    } else {
      logger.info(
        `Throttling message for device ${deviceId}. Last processed at ${new Date(
          lastProcessedTime
        ).toISOString()}.`
      );
    }
  } catch (err) {
    console.error(
      `Error processing MQTT message for topic ${topic}:`,
      err
    );
    logger.error("Error processing MQTT message:", err);
  }
});

mqttSubscriber.on("connect", () => {
  logger.info("MQTT connected");
  initializeSubscriptions();
});

mqttSubscriber.on("close", () => {
  logger.info("MQTT connection closed");
  activeSubscriptions.clear();
  deviceConfigurations.clear();
  lastProcessedMessageTime.clear();
});

mqttSubscriber.on("error", (err) => {
  logger.error("MQTT error:", err);
});

module.exports = {
  mqttSubscriber,
  subscribeToDevice,
  unsubscribeFromDevice,
  initializeSubscriptions,
  updateDeviceDataInterval,
};
