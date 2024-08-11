const express = require("express");
const router = express.Router();
const { body } = require("express-validator");
const {
  signIn,
  signUp,
  updateProfile,
  tokenIsValid,
  getUser,
} = require("../controllers/userAuthController");

const userValidation = [
  body("name").notEmpty().withMessage("Name is required"),
  body("email").isEmail().withMessage("Invalid email format"),
  body("password")
    .isLength({ min: 5 })
    .withMessage("Password must be at least 5 characters long"),
];

const auth = require("../middleware/auth");

// Sign-in route
router.post("/signin", signIn);

// Sign-up route
router.post("/signup", userValidation, signUp);

// Update user route
// router.post("/update", userValidation, updateProfile);//yeh removed hai iska code likhna hai
// Sign-in route
router.post("/signin", signIn);

// Sign-up route
router.post("/signup", userValidation, signUp);

//tokenIs Valid route
router.post("/tokenIsValid", tokenIsValid);

//ise get request bna diya ha
//get user data
router.get("/getUser", getUser);

module.exports = router;
