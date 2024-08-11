const User = require("../models/user");

// Add Doctor to Favorites
exports.addFavorite = async (req, res) => {
  try {
    const { userId, doctorId } = req.body;

    if (!userId || !doctorId) {
      return res
        .status(400)
        .json({ message: "User ID and Doctor ID are required" });
    }

    const user = await User.findById(userId);

    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }

    if (user.favorites.includes(doctorId)) {
      return res
        .status(400)
        .json({ message: "Doctor is already in favorites" });
    }

    user.favorites.push(doctorId);
    await user.save();

    res
      .status(200)
      .json({ message: "Doctor added to favorites successfully", user });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: "Server error" });
  }
};

// Remove Doctor from Favorites
exports.removeFavorite = async (req, res) => {
  try {
    const { userId, doctorId } = req.body;

    if (!userId || !doctorId) {
      return res
        .status(400)
        .json({ message: "User ID and Doctor ID are required" });
    }

    const user = await User.findById(userId);

    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }

    if (!user.favorites.includes(doctorId)) {
      return res.status(400).json({ message: "Doctor is not in favorites" });
    }

    user.favorites = user.favorites.filter(
      (fav) => fav.toString() !== doctorId
    );
    await user.save();

    res
      .status(200)
      .json({ message: "Doctor removed from favorites successfully", user });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: "Server error" });
  }
};

// Get User's Favorite Doctors
exports.getFavorites = async (req, res) => {
  try {
    const { userId } = req.params;

    if (!userId) {
      return res.status(400).json({ message: "User ID is required" });
    }

    const user = await User.findById(userId).populate("favorites");

    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }

    res.status(200).json({ favorites: user.favorites });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: "Server error" });
  }
};
