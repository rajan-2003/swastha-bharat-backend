const express = require("express");
const router = express.Router();
const {
  addNotification,
  scheduleNotification,
  updateNotificationStatus,
  getUserNotifications,
  deleteNotification,
  sendPushnotification,
} = require("../controllers/notificationController");
const jwt = require("jsonwebtoken");
const userSchema = require("../models/user");
const authMiddleware = require("../middleware/authMiddleware");
const { error } = require("console");
const JWT_SECRET = process.env.JWT_SECRET;
const { body, validationResult } = require("express-validator");
const bcrypt = require("bcrypt");
const User = require("../models/user");

router.post("/push-notification", sendPushnotification);

router.post("/add", addNotification);

// Schedule a notification
router.post("/schedule", scheduleNotification);

// Update notification status
router.post("/update", updateNotificationStatus);

// Get all notifications for a user
router.get("/:userId", getUserNotifications);

// Delete a notification
router.delete("/:notificationId", deleteNotification);

module.exports = router;
