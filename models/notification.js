// models/notification.js
const mongoose = require("mongoose");

// Define the notification schema
const notificationSchema = new mongoose.Schema({
  user_id: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "User",
    required: true,
  },
  msg: { type: String, required: true },
  seen: { type: Boolean, default: false },
  timestamp: { type: Date, default: Date.now }, // Add timestamp field
});

// Create and export the model
module.exports = mongoose.model("Notification", notificationSchema);
