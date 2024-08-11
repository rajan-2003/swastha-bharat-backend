const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
// const { check, validationResult } = require("express-validator");
const auth = require("../middleware/auth");

// const bcrypt = require("bcryptjs");
// const jwt = require("jsonwebtoken");
const { check, validationResult } = require("express-validator");
const Doctor = require("../models/doctor"); // Make sure to import the Doctor model
// const auth = require("../middleware/auth");

exports.registerDoctor = [
  // Validation checks
  check("email", "Please include a valid email").isEmail(),
  check(
    "password",
    "Please enter a password with 6 or more characters"
  ).isLength({ min: 6 }),

  // Middleware function
  async (req, res) => {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    const { email, password, name } = req.body;

    try {
      let doctor = await Doctor.findOne({ email });

      if (doctor) {
        return res.status(400).json({ msg: "Doctor already exists" });
      }

      doctor = new Doctor({
        email,
        password,
        name,
      });

      const salt = await bcrypt.genSalt(10);
      doctor.password = await bcrypt.hash(password, salt);

      await doctor.save();

      const payload = {
        doctor: {
          id: doctor.id,
        },
      };

      jwt.sign(
        payload,
        process.env.JWT_SECRET,
        { expiresIn: "5h" },
        (err, token) => {
          if (err) throw err;
          res.json({ token });
        }
      );
    } catch (err) {
      console.error(err.message);
      res.status(500).send("Server error");
    }
  },
];

exports.loginDoctor = async (req, res) => {
  const { email, password } = req.body;

  // Validate input (optional, based on your validation setup)
  // If you have validation logic, ensure to include it here

  try {
    // Check if the doctor exists
    const doctor = await Doctor.findOne({ email });
    if (!doctor) {
      return res.status(400).json({ msg: "Invalid email or password" });
    }

    // Check if the password matches
    const isMatch = await bcrypt.compare(password, doctor.password);
    if (!isMatch) {
      return res.status(400).json({ msg: "Invalid email or password" });
    }

    // Create payload and sign the token
    const payload = {
      doctor: {
        id: doctor.id,
      },
    };

    jwt.sign(
      payload,
      process.env.JWT_SECRET,
      { expiresIn: "5h" }, // Adjust the expiration time as needed
      (err, token) => {
        if (err) {
          console.error(err.message);
          return res.status(500).json({ msg: "Server error" });
        }
        res.json({ token, doctor });
      }
    );
  } catch (err) {
    console.error(err.message);
    res.status(500).send("Server error");
  }
};

exports.logoutDoctor = (req, res) => {
  try {
    // Optional: Handle logout logic if needed
    // Example: Invalidate token on the client-side or handle session management

    res.json({ msg: "Logout successful" });
  } catch (err) {
    console.error(err.message);
    res.status(500).send("Server error");
  }
};
