const mongoose = require("mongoose");

const doctorSchema = new mongoose.Schema({
  name: { type: String, required: true },
  email: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  specialty: { type: String },
  image: String,
  description: String,
  workExperience: Number,
  location: {
    type: {
      type: String,
      enum: ["Point"],
      required: true,
    },
    coordinates: {
      type: [Number],
      required: true,
    },
    address: { type: String }, // Optional address
  },
  clinicName: String,
  numberOfPatientsViewed: Number,
  experience: Number,
  rating: Number,
  reviewCount: Number,
  availableDateTimeSlots: [String],
});

// Ensure the location field is treated as a GeoJSON point
doctorSchema.index({ location: "2dsphere" });

// Pre-save hook to parse location string
doctorSchema.pre("save", function (next) {
  if (typeof this.location === "string") {
    const [latitude, longitude] = this.location
      .split(",")
      .map((coord) => parseFloat(coord.trim()));
    this.location = {
      type: "Point",
      coordinates: [longitude, latitude], // [longitude, latitude]
      address: this.location.address || "", // Handle empty or undefined address
    };
  }
  next();
});

module.exports = mongoose.model("Doctor", doctorSchema);

// const mongoose = require("mongoose");

// const DoctorSchema = new mongoose.Schema({
//   name: { type: String, required: true },
//   email: { type: String, required: true, unique: true },
//   password: { type: String, required: true },
//   specialty: { type: String },
//   image: String,
//   description: String,
//   workExperience: Number,
//   Lat: Number,
//   Long: Number,
//   location: { type: { type: String }, coordinates: [Number] },
//   clinicName: String,
//   numberOfPatientsViewed: Number,
//   experience: Number,
//   rating: Number,
//   reviewCount: Number,
//   availableDateTimeSlots: [String],
// });

// DoctorSchema.index({ location: "2dsphere" });

// module.exports = mongoose.model("Doctor", DoctorSchema);
