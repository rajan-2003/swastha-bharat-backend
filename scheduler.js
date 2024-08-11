const Bull = require("bull");
const axios = require("axios");

// Create a new queue with Redis configuration
const notificationQueue = new Bull("notificationQueue", {
  redis: {
    host: "localhost",
    port: 6379,
    retryStrategy: (times) => Math.min(times * 50, 2000),
    connectTimeout: 10000,
  },
});

// Handle queue errors
notificationQueue.on("error", (error) => {
  console.error("Queue error:", error.message);
});

// Process jobs in the queue
//process starts when the assign delay with a particular process mets the condition
notificationQueue.process(async (job) => {
  const { fcmToken, title, body } = job.data;
  try {
    const response = await axios.post(
      "http://localhost:5000/user/notifications/push-notification",
      { fcmToken, title, body },
      { headers: { "Content-Type": "application/json" } }
    );
    console.log("Notification sent:", response.data);
  } catch (error) {
    console.error("Error sending notification:", error.message);
  }
});

// Function to add a job
//it adds the process in the queue aalong with the delay
async function addNotificationJob(fcmToken, delayInMinutes, title, body) {
  try {
    if (!fcmToken || typeof fcmToken !== "string" || fcmToken.trim() === "") {
      console.error("add notification job");
      return res.status(400).send({ message: "Invalid FCM token" });
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
      // Optionally: Remove all jobs and metadata if you need a fresh start
      // await notificationQueue.empty();
    } else {
      console.log(
        `Queue status: ${waitingCount} waiting, ${activeCount} active, ${delayedCount} delayed.`
      );
    }
  } catch (error) {
    console.error("Error checking queue status:", error.message);
  }
}

// 10 seconds
const interval = 30000;

setInterval(async () => {
  await checkQueueStatus();
}, interval);

console.log("Started checking queue status every 30 seconds.");

module.exports = addNotificationJob;
