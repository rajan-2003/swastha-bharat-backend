const Doctor = require("../models/doctor"); // Ensure this path is correct
const haversine = require("../utils/haversine"); // If you use haversine function for distance calculation

// Get all doctors
const getAllDoctor = async (req, res) => {
  try {
    // Fetch all doctors
    const doctors = await Doctor.find();
    // Send the list of doctors as JSON
    res.json(doctors);
  } catch (err) {
    console.error(err.message);
    res.status(500).send("Server error");
  }
};
// Get doctors by specialty
const speciality = async (req, res) => {
  const { specialty } = req.query;

  if (!specialty) {
    return res.status(400).json({ msg: "Specialty is required" });
  }

  try {
    // Use case-insensitive regex to match the specialty
    const doctors = await Doctor.find({
      specialty: new RegExp(specialty, "i"),
    });

    if (doctors.length === 0) {
      return res
        .status(404)
        .json({ msg: "No doctors found with this specialty" });
    }

    // Send the list of doctors as JSON
    res.json(doctors);
  } catch (err) {
    console.error(err.message);
    res.status(500).send("Server error");
  }
};

const searchDoctors = async (req, res) => {
  try {
    const { name } = req.query;

    if (!name) {
      return res
        .status(400)
        .json({ error: "Name query parameter is required" });
    }

    const doctors = await Doctor.find({ name: new RegExp(name, "i") });

    if (doctors.length === 0) {
      return res.status(404).json({ message: "No doctors found" });
    }

    res.json(doctors);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

// Get doctors near a location
const nearestDoctor = async (req, res) => {
  const { latitude, longitude, radius } = req.query;

  if (!latitude || !longitude || !radius) {
    return res
      .status(400)
      .json({ msg: "Latitude, longitude, and radius are required" });
  }

  const userLat = parseFloat(latitude);
  const userLon = parseFloat(longitude);
  const searchRadius = parseFloat(radius);

  if (isNaN(userLat) || isNaN(userLon) || isNaN(searchRadius)) {
    return res
      .status(400)
      .json({ msg: "Invalid latitude, longitude, or radius" });
  }

  try {
    // Fetch doctors using geospatial query
    const nearbyDoctors = await Doctor.find({
      location: {
        $near: {
          $geometry: {
            type: "Point",
            coordinates: [userLon, userLat],
          },
          $maxDistance: searchRadius * 1000, // Convert km to meters
        },
      },
    });

    // Calculate distance for each doctor
    const result = nearbyDoctors.map((doctor) => {
      const [doclon, doclat] = doctor.location.coordinates;
      const distanceVal = haversine(doclat, doclon, userLat, userLon);
      return {
        name: doctor.name,
        specialty: doctor.specialty,
        rating: doctor.rating,
        distance: distanceVal.toFixed(0),
      };
    });

    // Send the list of doctors with distance as JSON
    res.json(result);
  } catch (err) {
    console.error(err.message);
    res.status(500).send("Server error");
  }
};

// Sign-in Controller DOctor
const signInDoctor = async (req, res) => {
  try {
    const { email, password } = req.body;
    const doctor = await Doctor.findOne({ email });

    if (!doctor) {
      return res
        .status(400)
        .json({ mssg: "Doctor with this email does not exist" });
    }
    const isMatch = await bcrypt.compare(password, doctor.password);
    if (!isMatch) {
      return res.status(400).json({ mssg: "Incorrect password" });
    }
    const doctorId = doctor._id;
    const token = jwt.sign({ id: doctorId }, JWT_SECRET);
    res.json({ token, ...doctor._doc });
  } catch (e) {
    console.error(e);
    return res.status(500).json({
      mssg: "Something went wrong while signin doctor",
      error: e.message,
    });
  }
};

// Sign-up Controller Doctor
// Sign-up Controller Doctor
const signUpDoctor = async (req, res) => {
  try {
    // Validate request data
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    const {
      name,
      email,
      password,
      description,
      workExperience,
      rating,
      reviewCount,
      location, // This will be in the format "latitude,longitude"
      clinicName,
      numberofPatientsViewed,
      availableDateTimeSlots,
    } = req.body;

    // Check if the email is already in use
    const existingDoctor = await Doctor.findOne({ email });
    if (existingDoctor) {
      return res.status(400).json({ mssg: "Doctor Email is already in use" });
    }

    // Hash the password
    const hashedPassword = await bcrypt.hash(password, 10);

    // Parse the location string into coordinates
    const [latitude, longitude] = location
      .split(",")
      .map((coord) => parseFloat(coord.trim()));
    const parsedLocation = {
      type: "Point",
      coordinates: [longitude, latitude], // Store coordinates as [longitude, latitude]
      address: "", // If you have an address, you can fill this in or leave it empty
    };

    // Create and save the new doctor
    const newDoctor = new Doctor({
      name,
      email,
      password: hashedPassword,
      description,
      workExperience,
      rating,
      reviewCount,
      location: parsedLocation, // Use the parsed location
      clinicName,
      numberofPatientsViewed,
      availableDateTimeSlots,
    });

    const doctorId = newDoctor._id;
    const token = jwt.sign({ id: doctorId }, JWT_SECRET);
    await newDoctor.save();
    res.json(newDoctor);
  } catch (e) {
    console.error(e);
    return res.status(500).json({
      mssg: "An error occurred while creating the user",
      error: e.message,
    });
  }
};

//tokenIsValid DOCTOR
const tokenIsValidDoctor = async (req, res) => {
  try {
    const token = req.header("x-auth-token");
    if (!token) return res.json(false);
    const verified = jwt.verify(token, "passwordKey");
    if (!verified) return res.json(false);

    const doctor = await Doctor.findById(verified.id);
    if (!doctor) return res.json(false);
    res.json(true);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};

// Get DOCTOR Data
const getDoctor = async (req, res) => {
  try {
    const doctor = await Doctor.findById(req.doctor);
    res.json({ ...doctor._doc, token: req.token });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};

//doctor by appointmtme
// Update saveDateTimeSlots to handle saving date and time slots
const saveDateTimeSlots = async (req, res) => {
  try {
    const { date, slots } = req.body;
    const doctorId = req.params.doctorId;

    // Validate input
    if (!date || !slots || !Array.isArray(slots)) {
      return res.status(400).send({ error: "Invalid input" });
    }

    // Find doctor by ID
    const doctor = await Doctor.findById(doctorId);
    if (!doctor) {
      return res.status(404).send({ error: "Doctor not found" });
    }

    // Call the instance method to update available date and time slots
    await doctor.updateAvailableDateTimeSlots(date, slots);

    res
      .status(200)
      .send({ message: "Date and time slots updated successfully." });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
};

// Endpoint to fetch available date and time slots
const availableDateTimeSlots = async (req, res) => {
  try {
    const doctorId = req.params.doctorId;

    // Find doctor by ID
    const doctor = await Doctor.findById(doctorId);
    if (!doctor) {
      return res.status(404).send({ error: "Doctor not found" });
    }

    // Send available date and time slots
    res
      .status(200)
      .send({ availableDateTimeSlots: doctor.availableDateTimeSlots });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
};

const removeDateTimeSlot = async (req, res) => {
  try {
    const { doctorId } = req.params;
    const { date, slot } = req.query;

    if (!date || !slot) {
      return res.status(400).json({ error: "Date and slot are required." });
    }

    // Convert date to a Date object
    const dateObj = new Date(date);

    // Check if the date is valid
    if (isNaN(dateObj.getTime())) {
      return res.status(400).json({ error: "Invalid date format" });
    }

    // Convert date to ISO string format for comparison (YYYY-MM-DD)
    const dateIsoString = dateObj.toISOString().split("T")[0];

    // Find the doctor by ID
    const doctor = await Doctor.findById(doctorId);

    if (!doctor) {
      return res.status(404).json({ error: "Doctor not found." });
    }

    // Find the slot document for the given date
    const slotDoc = doctor.availableDateTimeSlots.find((slotDoc) => {
      const slotDate = new Date(slotDoc.date);
      return (
        !isNaN(slotDate.getTime()) &&
        slotDate.toISOString().split("T")[0] === dateIsoString
      );
    });

    if (!slotDoc) {
      return res
        .status(404)
        .json({ error: "No available slots found for the given date." });
    }

    // Remove the specific slot from the slots array
    slotDoc.slots = slotDoc.slots.filter((s) => s !== slot);

    // If no slots are left, remove the entire date entry
    if (slotDoc.slots.length === 0) {
      doctor.availableDateTimeSlots = doctor.availableDateTimeSlots.filter(
        (s) => s.date.toISOString().split("T")[0] !== dateIsoString
      );
    }

    // Save the updated doctor document
    await doctor.save();

    return res.status(200).json({ message: "Slot removed successfully." });
  } catch (error) {
    console.error("Error removing slot:", error);
    return res.status(500).json({ error: "Internal server error." });
  }
};
module.exports = {
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
};
