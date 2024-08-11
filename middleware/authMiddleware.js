const jwt = require("jsonwebtoken");

const authMiddleware = (req, res, next) => {
  // Extract token from the Authorization header
  const authHeader = req.headers["authorization"];

  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res.status(401).json({ message: "No token, authorization denied" });
  }

  // Extract the token part from the header
  const token = authHeader.split(" ")[1];

  try {
    // Verify the token
    const decoded = jwt.verify(token, process.env.JWT_SECRET); // Replace with your secret
    req.userId = decoded.id; // Assuming the JWT payload contains an 'id' field
    next(); // Proceed to the next middleware or route handler
  } catch (error) {
    console.error("Token verification error:", error); // Log the error for debugging
    return res
      .status(401)
      .json({ message: "Token is not valid", error: error.message });
  }
};

module.exports = authMiddleware;
