const express = require("express");
const router = express.Router();
const {
  createAppointment,
  getUserAppointments,
  deleteAppointment,
} = require("../controllers/appointmentController");

// Create a new appointment
router.post("/appointments", createAppointment);

// Get all appointments for a specific user
router.get("/appointments/:userId", getUserAppointments);

// Delete an appointment by ID
router.delete("/delete/appointments/:id", deleteAppointment);

module.exports = router;
