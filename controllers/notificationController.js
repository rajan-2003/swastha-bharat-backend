const Notification = require("../models/notification");
const addNotificationJob = require("../scheduler.js");
const admin = require("firebase-admin");
const serviceAccount = require("../config/push-notification-key.json");
const { error } = require("console");
// const db = require('./firebaseAdmin');
// const fire_db = require("../config/fire_db.js");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});
const fire_db = admin.firestore();

const messaging = admin.messaging();
const STATIC_USER_ID = "staticUserId123";

// sendPushnotification
// Schedule a push notification
exports.sendPushnotification = async (req, res, next) => {
  try {
    const { fcmToken, title, body } = req.body;
    // Validate input
    if (!fcmToken || typeof fcmToken !== "string" || fcmToken.trim() === "") {
      console.error("Invalid FCM token");
      return res.status(400).send({ message: "Invalid FCM token" });
    }
    if (!title || typeof title !== "string" || title.trim() === "") {
      console.error("Invalid title ");
      return res.status(400).send({ message: "Invalid title" });
    }
    if (!body || typeof body !== "string" || body.trim() === "") {
      console.error("Invalid body");
      return res.status(400).send({ message: "Invalid body" });
    }
    await fire_db
      .collection("users")
      .doc(STATIC_USER_ID)
      .collection("notifications")
      .add({
        title,
        body,
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
        seen: false,
      });
    const message = {
      notification: {
        title,
        body,
      },
      data: {
        score: "850",
        time: "2:45",
      },
      android: {
        priority: "high",
      },
      token: fcmToken,
    };
    messaging
      .send(message)
      .then((response) => {
        console.log("Message sent successfully:", response);
        return res.status(200).send({ message: "Notification sent" });
      })
      .catch((err) => {
        console.error("Error sending message:", err);
        return res
          .status(500)
          .send({ message: "Failed to send notification", error: err.message });
      });
  } catch (err) {
    console.error("Unexpected error:", err);
    next(err);
  }
};

// Schedule a notification
exports.scheduleNotification = async (req, res) => {
  const { delayInMinutes = 0, fcmToken, title, body } = req.body;
  try {
    await addNotificationJob(fcmToken, delayInMinutes, title, body);
    res.status(200).json({ message: "Notification scheduled successfully" });
  } catch (error) {
    console.log(error);
    res.send(error);
  }
};

//add a notification
exports.addNotification = async (req, res) => {
  const { userId, msg } = req.body;
  try {
    const notification = new Notification({
      user_id: userId,
      msg: msg,
      seen: false,
    });

    await notification.save();
    res
      .status(200)
      .json({ message: "Notification added and pushed successfully" });
  } catch (error) {
    res.status(500).json({ error: "Failed to add notification" });
  }
};

// Update notification status
exports.updateNotificationStatus = async (req, res) => {
  const { userId, notificationId } = req.body;
  try {
    const notification = await Notification.findOne({
      user_id: userId,
      _id: notificationId,
    });
    if (!notification) {
      return res.status(404).json({ error: "Notification not found" });
    }
    notification.seen = true;
    await notification.save();
    res.status(200).json({ message: "Notification status updated" });
  } catch (error) {
    res.status(500).json({ error: "Failed to update notification status" });
  }
};

// Get all notifications for a user
exports.getUserNotifications = async (req, res) => {
  const { userId } = req.params;
  try {
    const notifications = await Notification.find({ user_id: userId }).sort({
      timestamp: -1,
    });
    res.status(200).json(notifications);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch notifications" });
  }
};

// Delete a notification
exports.deleteNotification = async (req, res) => {
  const { notificationId } = req.params;
  try {
    const result = await Notification.deleteOne({ _id: notificationId });
    if (result.deletedCount === 0) {
      return res.status(404).json({ error: "Notification not found" });
    }
    res.status(200).json({ message: "Notification deleted successfully" });
  } catch (error) {
    res.status(500).json({ error: "Failed to delete notification" });
  }
};
