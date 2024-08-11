require("dotenv").config();
const express = require("express");
const connectDB = require("./config/db");
const cookieParser = require("cookie-parser");
const bodyParser = require("body-parser"); // Import body-parser
const notificationRouter = require("./routes/notification");
const appointmentRoutes = require("./routes/appointment");
const favouritesRoutes = require("./routes/favourites");
const userRoutes = require("./routes/user");

const app = express();
app.use(cookieParser());

// Connect Database
connectDB();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

app.use("/api/doctors", require("./routes/doctor"));

app.use("/user/notifications", notificationRouter);
app.use("/", appointmentRoutes);
app.use("/favourites", favouritesRoutes);
app.use("/userAuth", userRoutes);

const PORT = process.env.PORT || 5000;

app.listen(PORT, () => console.log(`Server started on port ${PORT}`));
