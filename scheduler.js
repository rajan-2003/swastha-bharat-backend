const Bull = require("bull");
const axios = require("axios");
const IORedis = require("ioredis");

// Your external Redis URL
const redisUrl = "rediss://red-cqsh9fij1k6c73fkrbdg:5ijVm5D5EbiSSo9iJfrYFlX3Dc4Mwg1G@oregon-redis.render.com:6379";

// Create a new ioredis client with adjusted options
const redisClient = new IORedis(redisUrl, {
  tls: {
    rejectUnauthorized: false,
  },
  enableReadyCheck: false,  // Disable ready check to avoid conflicts with Bull
  maxRetriesPerRequest: null, // Ensure no max retries per request
});

// Create a new queue with the external Redis client
const notificationQueue = new Bull("notificationQueue", {
  createClient: (type) => {
    switch (type) {
      case "client":
        return redisClient;
      case "subscriber":
        return redisClient.duplicate({
          enableReadyCheck: false,
          maxRetriesPerRequest: null,
        });
      default:
        return redisClient;
    }
  },
});

// Handle queue errors
notificationQueue.on("error", (error) => {
  console.error("Queue error:", error.message);
});

// Process jobs in the queue
notificationQueue.process(async (job) => {
  const { fcmToken, title, body } = job.data;
  try {
    const response = await axios.post(
      "https://swastha-bharat-backend.onrender.com/user/notifications/push-notification",
      { fcmToken, title, body },
      { headers: { "Content-Type": "application/json" } }
    );
    console.log("Notification sent:", response.data);
  } catch (error) {
    console.error("Error sending notification:", error.message);
  }
});

// Function to add a job
async function addNotificationJob(fcmToken, delayInMinutes, title, body) {
  try {
    if (!fcmToken || typeof fcmToken !== "string" || fcmToken.trim() === "") {
      console.error("Invalid FCM token");
      return;
    }
    const delay = delayInMinutes * 60 * 1000; // Convert minutes to milliseconds
    await notificationQueue.add({ fcmToken, title, body }, { delay: delay });
    console.log(
      `Notification job scheduled for ${delayInMinutes} minutes from now.`
    );
  } catch (error) {
    console.error("Error scheduling notification job:", error.message);
  }
}

// Function to check if the queue is empty
async function checkQueueStatus() {
  try {
    const waitingCount = await notificationQueue.getWaitingCount();
    const activeCount = await notificationQueue.getActiveCount();
    const delayedCount = await notificationQueue.getDelayedCount();

    if (waitingCount === 0 && activeCount === 0 && delayedCount === 0) {
      console.log("Queue is empty. Cleaning up jobs.");
      await notificationQueue.clean(0, "completed");
      await notificationQueue.clean(0, "failed");
    } else {
      console.log(
        `Queue status: ${waitingCount} waiting, ${activeCount} active, ${delayedCount} delayed.`
      );
    }
  } catch (error) {
    console.error("Error checking queue status:", error.message);
  }
}

// 30 seconds
const interval = 30000;

setInterval(async () => {
  await checkQueueStatus();
}, interval);

console.log("Started checking queue status every 30 seconds.");

module.exports = addNotificationJob;
