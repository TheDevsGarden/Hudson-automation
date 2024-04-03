const express = require("express");
const path = require("path");
const mysql = require("mysql");
const app = express();
const port = 3030;

// Create a connection without a database
const db = mysql.createConnection({
  host: "127.0.0.1",
  user: "root",
  password: "Ahuntsic@2023!", //replace with your own password
});

// Connect to the database
db.connect((err) => {
  if (err) throw err;
  console.log("Connected to MySQL.");

  // Create a new database
  const sqlCreateDatabase = "CREATE DATABASE IF NOT EXISTS HudsonDBTest";
  db.query(sqlCreateDatabase, (err, result) => {
    if (err) throw err;
    console.log("Database created.");

    // Switch to the database
    db.changeUser({ database: "HudsonDBTest" }, function (err) {
      if (err) throw err;

      // Create a new table
      const sqlCreateTable = "CREATE TABLE IF NOT EXISTS testTable (id INT AUTO_INCREMENT, name VARCHAR(255), PRIMARY KEY(id))";
      db.query(sqlCreateTable, (err, result) => {
        if (err) throw err;
        console.log("Table created.");

        // Insert dummy values into the table
        const sqlInsert = "INSERT INTO testTable (name) VALUES ('Dummy')";
        db.query(sqlInsert, (err, result) => {
          if (err) throw err;
          console.log("Dummy values inserted.");
        });
      });
    });
  });
});

app.get("/", (req, res) => {
  res.send("Hudson Server is running");
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
