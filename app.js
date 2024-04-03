const fs = require("fs"); //I will be using the file system
const readline = require("readline");
const express = require("express"); //I will open a node server which will talk to the SQL database server
const path = require("path");
const mysql = require("mysql");
const app = express();
const port = 3030;

const filepath = path.join(__dirname, "data", "stocks.csv");

app.get("/", (req, res) => {
  res.send("Hudson Server is running");
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});

// Create a connection without a database
const connection = mysql.createConnection({
  host: "127.0.0.1", //I am using mySQL on localhost. If you are using a different server, replace this with the server's IP address.
  user: "root",
  password: "Ahuntsic@2023!", //replace with your own password
});

// Create the database
connection.connect((err) => {
  if (err) throw err;
  console.log("Connected to MySQL.");

  // Create a new database
  const sqlCreateDatabase = "CREATE DATABASE IF NOT EXISTS HudsonDBTest";
  connection.query(sqlCreateDatabase, (err, result) => {
    if (err) throw err;
    console.log("Database created.");

    // Switch to the database
    connection.changeUser({ database: "HudsonDBTest" }, function (err) {
      if (err) throw err;
    });
  });
});

function readStockDataFromCSV(filePath) {
  return new Promise((resolve, reject) => {
    const stockData = [];
    const fileStream = fs.createReadStream(filePath);

    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    });

    let isFirstLine = true;

    rl.on("line", (line) => {
      if (isFirstLine) {
        isFirstLine = false;
        return;
      }

      const [Symbol, GivenDate, Open, High, Low, Close, Volume] = line.split(",");

      // Convert the date
      const dateParts = GivenDate.split("/");
      const year = dateParts[2];
      // Add leading zero if needed
      const month = ("0" + dateParts[0]).slice(-2);
      const day = ("0" + dateParts[1]).slice(-2);
      const formattedDate = `${year}-${month}-${day}`;

      console.log("Parsed Stock Data:", stockData[stockData.length - 1]);

      stockData.push({
        Symbol,
        // Construct a Date object
        Date: new Date(formattedDate),
        Open: parseFloat(Open),
        High: parseFloat(High),
        Low: parseFloat(Low),
        Close: parseFloat(Close),
        Volume: parseFloat(Volume),
      });
    });

    rl.on("close", () => {
      resolve(stockData);
    });

    rl.on("error", (err) => {
      reject(err);
    });
  });
}

// Function to calculate the daily percentage change in closing price for each stock
function calculateDailyPercentageChange(stockData) {
  const dailyPercentageChange = new Map();

  for (const data of stockData) {
    const { Symbol, Date, Close } = data;
    // Fomat the date as YYYY-MM-DD for MySQL
    const dateStr = Date.toISOString().split("T")[0];

    if (!dailyPercentageChange.has(Symbol)) {
      dailyPercentageChange.set(Symbol, new Map());
    }

    const symbolMap = dailyPercentageChange.get(Symbol);

    // if (!symbolMap.has(dateStr)) {
    //   symbolMap.set(dateStr, Close); // Initialize with previous close
    // }

    if (!symbolMap.has(dateStr)) {
      // Find the first instance of this symbol
      const firstClose = findFirstCloseForSymbol(stockData, Symbol, dateStr);

      if (firstClose !== null) {
        symbolMap.set(dateStr, firstClose); // Initialize with the actual first close
      } else {
        // Handle the case where no previous close can be found (see considerations below)
        symbolMap.set(dateStr, Close); // You might choose a different default here
      }
    }
    const prevClose = symbolMap.get(dateStr);
    const percentageChange = prevClose !== 0 ? ((Close - prevClose) / prevClose) * 100 : 0;
    // const percentageChange = ((Close - prevClose) / prevClose) * 100;

    console.log(Symbol, dateStr, "Percentage Change:", percentageChange.toFixed(2));

    symbolMap.set(dateStr, Close); // Update the current day's close price
    dailyPercentageChange.get(Symbol).set(dateStr, percentageChange);
  }

  return dailyPercentageChange;
}

function findFirstCloseForSymbol(stockData, symbol, dateStr) {
  for (let i = 0; i < stockData.length; i++) {
    if (stockData[i].Symbol === symbol && stockData[i].Date.toISOString().split("T")[0] < dateStr) {
      return stockData[i].Close;
    }
  }
  return null;
}

// Function to identify the stock with the largest absolute percentage change for each day
function findStockWithLargestMove(dailyPercentageChange) {
  const largestMovePerDay = new Map();

  for (const [symbol, dateMap] of dailyPercentageChange) {
    for (const [date, percentageChange] of dateMap) {
      const currentLargestMove = largestMovePerDay.get(date);

      if (!currentLargestMove || Math.abs(percentageChange) > Math.abs(currentLargestMove.percentageChange)) {
        largestMovePerDay.set(date, { symbol, percentageChange });
      }
    }
  }

  return largestMovePerDay;
}

// Function to generate a report summarizing the stock with the biggest move for each day
function generateReport(largestMovePerDay) {
  let report = "GivenDate,Symbol,Percentage Change\n";

  for (const [date, { symbol, percentageChange }] of largestMovePerDay) {
    report += `${date},${symbol},${percentageChange.toFixed(2)}%\n`;
  }

  return report;
}

// Function to find the average percentage change for each stock over the entire dataset
function calculateAveragePercentageChange(dailyPercentageChange) {
  const averagePercentageChange = new Map();

  for (const [symbol, dateMap] of dailyPercentageChange) {
    let sum = 0;
    let count = 0;

    for (const percentageChange of dateMap.values()) {
      sum += percentageChange;
      count++;
    }

    averagePercentageChange.set(symbol, sum / count);
  }

  return averagePercentageChange;
}

// Function to save the generated report and average percentage change data to a MySQL database
async function saveToDatabase(report, averagePercentageChange) {
  // Create the table to store the report data
  const createReportTableQuery = `
    CREATE TABLE IF NOT EXISTS report (
      id INT AUTO_INCREMENT PRIMARY KEY,
      date VARCHAR(50),
      symbol VARCHAR(10),
      percentage_change FLOAT
    )
  `;

  //

  connection.query(createReportTableQuery, (err, result) => {
    if (err) {
      console.error("Error creating the report table:", err);
      return;
    }
    console.log("Report table created or already exists.");
  });

  // Insert the report data into the database
  const reportLines = report.trim().split("\n").slice(1);
  const reportInsertQuery = "INSERT INTO report (date, symbol, percentage_change) VALUES ?";
  const reportValues = reportLines.map((line) => {
    const [date, symbol, percentageChange] = line.split(",");
    return [date, symbol, parseFloat(percentageChange)];
  });

  console.log("Values to be inserted:", reportValues);

  // function formatDateForMySQL(dateStr) {
  //   const [year, month, day] = dateStr.split("-");
  //   return `${year}-${month}-${day}`;
  // }
  connection.query(reportInsertQuery, [reportValues], (err, result) => {
    // connection.query(
    //   reportInsertQuery,
    //   [
    //     reportValues.map((row) => [
    //       formatDateForMySQL(row[0]), // Format the date
    //       row[1],
    //       row[2],
    //     ]),
    //   ],
    //   (err, result) => {
    if (err) {
      console.error("Error inserting report data:", err);
      return;
    }
    console.log(`${result.affectedRows} rows inserted into the report table.`);
  });

  // Create the table to store the average percentage change data
  const createAverageTableQuery = `
    CREATE TABLE IF NOT EXISTS average_percentage_change (
      id INT AUTO_INCREMENT PRIMARY KEY,
      symbol VARCHAR(10),
      average_change FLOAT
    )
  `;

  connection.query(createAverageTableQuery, (err, result) => {
    if (err) {
      console.error("Error creating the average percentage change table:", err);
      return;
    }
    console.log("Average percentage change table created or already exists.");
  });

  // Insert the average percentage change data into the database
  const averageInsertQuery = "INSERT INTO average_percentage_change (symbol, average_change) VALUES ?";
  const averageValues = Array.from(averagePercentageChange).map(([symbol, averageChange]) => [symbol, averageChange]);

  connection.query(averageInsertQuery, [averageValues], (err, result) => {
    if (err) {
      console.error("Error inserting average percentage change data:", err);
      return;
    }
    console.log(`${result.affectedRows} rows inserted into the average percentage change table.`);
  });

  // Close the database connection
  connection.end((err) => {
    if (err) {
      console.error("Error closing the database connection:", err);
      return;
    }
    console.log("Database connection closed.");
  });
}
// Function to create an SQL script to count the number of days a symbol was the top-performing asset
function generateTopPerformingAssetQuery() {
  const query = `
      SELECT symbol, COUNT(*) AS top_performing_days
      FROM report
      GROUP BY symbol
      ORDER BY top_performing_days DESC;
    `;

  return query;
}

// Main function to orchestrate the data processing and reporting
async function main() {
  // const filePath = path.join(__dirname, "data", "stocks.csv");
  const filePath = "./data/stocks.csv";
  const stockData = await readStockDataFromCSV(filePath);

  const dailyPercentageChange = calculateDailyPercentageChange(stockData);
  const largestMovePerDay = findStockWithLargestMove(dailyPercentageChange);
  const report = generateReport(largestMovePerDay);
  const averagePercentageChange = calculateAveragePercentageChange(dailyPercentageChange);

  await saveToDatabase(report, averagePercentageChange);

  const topPerformingAssetQuery = generateTopPerformingAssetQuery();
  console.log("SQL query to count the number of days a symbol was the top-performing asset:");
  console.log(topPerformingAssetQuery);
}

main().catch((err) => {
  console.error("An error occurred:", err);
});
