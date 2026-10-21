require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const cors = require("cors");
const morgan = require("morgan");
const http = require("http");

const logger = require("./config/logger.js");
const pool = require("./config/postgre.js");
const { HTTP_PORT } = require("./config/socketConfig"); // Use same port or .env
const {initializeSocket} = require("./socket");

const app = express();
const server = http.createServer(app); // 👈 Create server from Express app

// Attach socket.io to this server
initializeSocket(server);

// Middleware
app.use(helmet());
app.use(cors());
app.use(express.json());
app.use(morgan("dev"));

// Routes
const exampleRoutes = require("./routes/example.route.js");
const v1Routes = require("./routes/v1");
app.use("/api/example", exampleRoutes);
app.use("/v1", v1Routes);

// Swagger
const setupSwagger = require("./config/swagger.js");
setupSwagger(app);

pool
  .checkDatabaseConnection()
  .then(() => pool.checkAndCreateTables())
  .then(() => {
    require("./worker/hyperbaseFlushWorker.js");
    require("./worker/historyPrewarmWorker.js");
    const { mqttSubscriber } = require("./mqtt/subscriber.js");

    server.listen(HTTP_PORT, () => {
      logger.info(`Server (HTTP + Socket.IO) running on port ${HTTP_PORT}`);
    });
  })
  .catch((err) => {
    logger.error("Failed to initialize database or start server:", err);
    process.exit(1);
  });

module.exports = { app, pool };
