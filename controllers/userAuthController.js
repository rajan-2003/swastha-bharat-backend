const jwt = require("jsonwebtoken");
const bcrypt = require("bcrypt");
const { validationResult } = require("express-validator");
const User = require("../models/user");

const JWT_SECRET = process.env.JWT_SECRET;

// Sign-in Controller
exports.signIn = async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = await User.findOne({ email });

    if (!user) {
      return res
        .status(400)
        .json({ mssg: "User with this email does not exist" });
    }
    const isMatch = await bcrypt.compare(password, user.password);
    if (!isMatch) {
      return res.status(400).json({ mssg: "Incorrect password" });
    }
    const userId = user._id;
    const token = jwt.sign({ id: userId }, JWT_SECRET);
    res.json({ token, ...user._doc });
  } catch (e) {
    console.error(e);
    return res
      .status(500)
      .json({ mssg: "Something went wrong", error: e.message });
  }
};

// Sign-up Controller
exports.signUp = async (req, res) => {
  try {
    // Validate request data
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    const { name, email, password } = req.body;

    // Check if the email is already in use
    const existingUser = await User.findOne({ email });
    if (existingUser) {
      return res.status(400).json({ mssg: "Email is already in use" });
    }

    // Hash the password
    const hashedPassword = await bcrypt.hash(password, 10);

    // Create and save new user
    const newUser = new User({
      name,
      email,
      password: hashedPassword,
    });
    const userId = newUser._id;
    const token = jwt.sign({ userId }, JWT_SECRET);
    await newUser.save();
    res.json(newUser);
  } catch (e) {
    console.error(e);
    return res.status(500).json({
      mssg: "An error occurred while creating the user",
      error: e.message,
    });
  }
};
//idhar changes kre ha
//tokenIsValid
exports.tokenIsValid = async (req, res) => {
  try {
    const token = req.header("x-auth-token");
    if (!token) return res.json(false);
    const verified = jwt.verify(token, process.env.JWT_SECRET);
    if (!verified) return res.json(false);

    const user = await User.findById(verified.id);
    if (!user) return res.json(false);
    res.json(true);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};

// Get User Data

//idhar changes kre gye ha

exports.getUser = async (req, res) => {
  try {
    // Extract the token from headers
    const token = req.header("x-auth-token");

    if (!token) return res.status(401).json({ error: "No token provided" });

    // Verify the token
    const verified = jwt.verify(token, process.env.JWT_SECRET);

    if (!verified) return res.status(401).json({ error: "Invalid token" });

    // Find the user by ID from the token
    const user = await User.findById(verified.id);

    if (!user) return res.status(404).json({ error: "User not found" });
    // Respond with user data and token
    res.json({ ...user._doc, token });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};
