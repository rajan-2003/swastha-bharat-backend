const express = require("express");
const router = express.Router();
const {
  addFavorite,
  removeFavorite,
  getFavorites,
} = require("../controllers/favouriteController");

// Add Doctor to Favorites
//changes are there
router.post("/add-favorite", addFavorite);

// Remove Doctor from Favorites
router.post("/remove-favorite", removeFavorite);

// Get User's Favorite Doctors
router.get("/favorites/:userId", getFavorites);

module.exports = router;
