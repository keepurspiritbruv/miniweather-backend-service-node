const { client } = require("../../config/scylla");
const redis = require("../../config/redis");
const crypto = require("crypto");

class WeatherDataRepositoryScyllaDB {
  /**
   * Get minute-by-minute averages of weather data within a time range
   * @param {Object} options - Query options
   * @param {Date} options.startTime - Start of time range
   * @param {Date} options.endTime - End of time range
   * @param {string} options.tableName - Target records table name
   * @param {string} [options.timezone='UTC'] - Timezone (ScyllaDB handles timestamps as UTC by default)
   * @returns {Promise<Array>} Array of minute-averaged records
   */
  async getMinuteAverages({
    startTime,
    endTime,
    tableName,
    collectionId,
    timezone = "UTC",
    fields = [],
  }) {
    const sanitizedTable = this._sanitizeTableName(tableName);

    const cacheKey = this._generateCacheKey({
      prefix: "minute_avg",
      table: sanitizedTable,
      collectionId,
      startTime,
      endTime,
      timezone,
      fields,
    });

    const cached = await redis.get(cacheKey);
    if (cached) {
      return JSON.parse(cached);
    }

    // Scope to the device partition (collectionId) so Scylla reads one partition
    // instead of scanning the whole table.
    const query = `
      SELECT "_updated_at", ${fields.join(", ")}
      FROM ${sanitizedTable}
      WHERE "_collection_id" = ? AND "_updated_at" >= ? AND "_updated_at" <= ?
      ALLOW FILTERING
    `;
    const params = [collectionId, startTime, endTime];

    const res = await client.execute(query, params, { prepare: true });

    const minuteAverages = new Map();

    // Iterate every page (auto-paging); `res.rows` would only expose the first page.
    for await (const row of res) {
      const date = new Date(row._updated_at);
      const minuteKey = new Date(
        date.toLocaleString("en-US", { timeZone: timezone })
      )
        .toISOString()
        .substring(0, 16); 

      if (!minuteAverages.has(minuteKey)) {
        minuteAverages.set(minuteKey, {
          count: 0,
          values: fields.reduce((acc, field) => ({ ...acc, [field]: 0 }), {}),
        });
      }

      const current = minuteAverages.get(minuteKey);
      current.count++;
      for (const field of fields) {
        if (row[field] != null) {
          current.values[field] += row[field];
        }
      }
    }

    const result = Array.from(minuteAverages.entries())
      .map(([minute, data]) => {
        const averaged = {};
        for (const field of fields) {
          averaged[field] = data.count > 0 ? data.values[field] / data.count : null;
        }
        return { minute: new Date(minute + ":00.000Z"), ...averaged };
      })
      .sort((a, b) => a.minute.getTime() - b.minute.getTime());

    await redis.set(cacheKey, JSON.stringify(result), "EX", 300);

    return result;
  }

  /**
   * Get weather data within a time range
   * @param {Object} options - Query options
   * @param {Date} options.startTime - Start of time range
   * @param {Date} options.endTime - End of time range
   * @param {string} options.tableName - Target records table name
   * @param {number} [options.limit=1000] - Maximum number of records to return
   * @returns {Promise<Array>} Array of raw records
   */
  async getDataInTimeRange({ startTime, endTime, tableName, collectionId, limit = 1000 }) {
    const sanitizedTable = this._sanitizeTableName(tableName);

    // In ScyllaDB, filtering on non-primary key columns (like _updated_at if not part of PK)
    // requires ALLOW FILTERING, which is generally discouraged for performance on large datasets.
    // For time-series data, it's common to have a clustering key based on time.
    const query = `
        SELECT *
        FROM ${sanitizedTable}
        WHERE "_collection_id" = ? AND "_updated_at" >= ? AND "_updated_at" <= ?
        LIMIT ?
        ALLOW FILTERING
    `;
    const params = [collectionId, startTime, endTime, limit];

    const res = await client.execute(query, params, { prepare: true });
    return res.rows;
  }

  /**
   * Get the most recent `_updated_at` timestamp available for a device.
   * Uses a server-side max() aggregate so only ONE row is returned — the driver
   * `fetchSize` paging bug (first page only) cannot hide the true latest row.
   * @param {Object} options
   * @param {string} options.tableName - Target records table name
   * @param {Uuid|string} options.collectionId - Device partition key (_collection_id)
   * @returns {Promise<string|null>}
   */
  async getLatestTimestamp({ tableName, collectionId }) {
    const sanitizedTable = this._sanitizeTableName(tableName);

    // Aggregating a partition needs no ALLOW FILTERING; Scylla computes max server-side.
    const query = `
      SELECT max("_updated_at") AS latest
      FROM ${sanitizedTable}
      WHERE "_collection_id" = ?
    `;
    const res = await client.execute(query, [collectionId], { prepare: true });

    const latest = res.rows[0]?.latest;
    return latest ? new Date(latest).toISOString() : null;
  }

  /**
   * Insert new weather data record into specified table
   * @param {Object} options
   * @param {string} options.tableName - Target table
   * @param {number} options.pressure
   * @param {number} options.wind_speed
   * @param {number} options.temperature
   * @param {number} options.rainfall
   * @param {string} [options.createdBy]
   */
  async create({
    tableName,
    pressure,
    wind_speed,
    temperature,
    rainfall,
    createdBy = null,
  }) {
    const sanitizedTable = this._sanitizeTableName(tableName);
    const id = crypto.randomUUID();

    const query = `
      INSERT INTO ${sanitizedTable}
        (id, _created_by, _updated_at, pressure, wind_speed, temperature, rainfall)
      VALUES
        (?, ?, toTimestamp(now()), ?, ?, ?, ?)
    `;
    const params = [id, createdBy, pressure, wind_speed, temperature, rainfall];
    await client.execute(query, params, { prepare: true });

    // Optional: Clear related minute_avg cache
    await this._clearMinuteCache(sanitizedTable);

    return {
      _id: id,
      _created_by: createdBy,
      _updated_at: new Date(),
      pressure,
      wind_speed,
      temperature,
      rainfall,
    };
  }

  _sanitizeTableName(tableName) {
    if (!/^records_[a-zA-Z0-9_]+$/.test(tableName)) {
      throw new Error("Invalid table name");
    }
    return tableName;
  }

  _generateCacheKey({ prefix, table, collectionId, startTime, endTime, timezone, fields }) {
    const raw = `${table}|${collectionId}|${startTime.toISOString()}|${endTime.toISOString()}|${timezone}|${fields.join(",")}`;
    const hash = crypto.createHash("sha256").update(raw).digest("hex");
    return `${prefix}:${table}:${hash}`;
  }

  async _clearMinuteCache(table) {
    const keys = await redis.keys(`minute_avg:${table}:*`);
    if (keys.length > 0) {
      await redis.del(keys);
    }
  }
}

module.exports = new WeatherDataRepositoryScyllaDB();
