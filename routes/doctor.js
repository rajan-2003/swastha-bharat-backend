const express = require("express");

const {
  getAllDoctor,
  speciality,
  nearestDoctor,
  signInDoctor,
  signUpDoctor,
  tokenIsValidDoctor,
  getDoctor,
  searchDoctors,
  saveDateTimeSlots,
  removeDateTimeSlot,
  availableDateTimeSlots,
} = require("../controllers/doctorController");

const {
  registerDoctor,
  loginDoctor,
  logoutDoctor,
} = require("../controllers/authController");

const admin = require("../middleware/auth_doctor");

const router = express.Router();

router.route("/register").post(registerDoctor);
router.route("/login").post(loginDoctor);
router.route("/logout").post(logoutDoctor);

router.get("/all", getAllDoctor);
router.get("/specialty", speciality);
router.get("/near", nearestDoctor);
router.get("/search-doctor", searchDoctors);

// Sign-in route
router.post("/signin", signInDoctor);

// Sign-up route
router.post("/signup", signUpDoctor);

//tokenIs Valid route
router.post("/tokenIsValid", tokenIsValidDoctor);

//get user data
router.post("/getDoctor", admin, getDoctor);

//appointments docotr

// Endpoint to save date and time slots
router.post("/:doctorId/saveDateTimeSlots", saveDateTimeSlots);

// Endpoint to fetch available date and time slots
router.get("/:doctorId/availableDateTimeSlots", availableDateTimeSlots);

// remove
router.delete("/:doctorId/removeDateTimeSlot", removeDateTimeSlot);

module.exports = router;
