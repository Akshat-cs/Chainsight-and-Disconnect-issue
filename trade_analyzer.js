const fs = require("fs");
const csv = require("csv-parser");

class TradeAnalyzer {
  constructor() {
    this.signerCounts = new Map();
    this.totalTrades = 0;
  }

  // Analyze the matched trades CSV file
  async analyzeCSV(csvFilePath) {
    return new Promise((resolve, reject) => {
      console.log(`Analyzing trades from: ${csvFilePath}`);

      fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on("data", (row) => {
          const signer = row.signer?.trim();

          if (signer) {
            // Increment count for this signer
            this.signerCounts.set(
              signer,
              (this.signerCounts.get(signer) || 0) + 1
            );
            this.totalTrades++;
          }
        })
        .on("end", () => {
          console.log(
            `Analysis complete. Processed ${this.totalTrades} trades.`
          );
          resolve();
        })
        .on("error", (error) => {
          console.error("Error reading CSV:", error);
          reject(error);
        });
    });
  }

  // Display results in console (summary only for large datasets)
  displayResults() {
    console.log("\n=== TRADE COUNT ANALYSIS ===\n");

    // Convert Map to array and sort by count (descending)
    const sortedSigners = Array.from(this.signerCounts.entries()).sort(
      (a, b) => b[1] - a[1]
    );

    console.log(`Total unique signers: ${this.signerCounts.size}`);
    console.log(`Total trades processed: ${this.totalTrades}`);
    console.log(
      `Average trades per signer: ${(
        this.totalTrades / this.signerCounts.size
      ).toFixed(2)}`
    );

    // Show top 5 for preview
    console.log("\n=== TOP 5 SIGNERS (Preview) ===");
    sortedSigners.slice(0, 5).forEach(([signer, count], index) => {
      const percentage = ((count / this.totalTrades) * 100).toFixed(2);
      console.log(`${index + 1}. ${signer} - ${count} trades (${percentage}%)`);
    });

    console.log("\n(Full results will be saved to CSV file)");
  }

  // Save results to a CSV file
  saveResultsToCSV(outputPath = "signer_trade_counts.csv") {
    const sortedSigners = Array.from(this.signerCounts.entries()).sort(
      (a, b) => b[1] - a[1]
    );

    let csvContent = "signer,trade_count\n";

    sortedSigners.forEach(([signer, count]) => {
      csvContent += `${signer},${count}\n`;
    });

    fs.writeFileSync(outputPath, csvContent);
    console.log(`\nSigner trade counts saved to: ${outputPath}`);
    console.log(`Created ${sortedSigners.length} rows (one per unique signer)`);
    return outputPath;
  }

  // Save detailed JSON report
  saveDetailedReport(outputPath = "trade_analysis_report.json") {
    const sortedSigners = Array.from(this.signerCounts.entries()).sort(
      (a, b) => b[1] - a[1]
    );

    const report = {
      analysis_timestamp: new Date().toISOString(),
      summary: {
        total_unique_signers: this.signerCounts.size,
        total_trades: this.totalTrades,
        average_trades_per_signer: parseFloat(
          (this.totalTrades / this.signerCounts.size).toFixed(2)
        ),
      },
      top_10_signers: sortedSigners.slice(0, 10).map(([signer, count]) => ({
        signer,
        trade_count: count,
        percentage: parseFloat(((count / this.totalTrades) * 100).toFixed(2)),
      })),
      all_signers: sortedSigners.map(([signer, count]) => ({
        signer,
        trade_count: count,
      })),
    };

    fs.writeFileSync(outputPath, JSON.stringify(report, null, 2));
    console.log(`Detailed report saved to: ${outputPath}`);
  }

  // Get statistics about trade distribution
  getStatistics() {
    const counts = Array.from(this.signerCounts.values()).sort((a, b) => b - a);

    return {
      total_signers: this.signerCounts.size,
      total_trades: this.totalTrades,
      max_trades: Math.max(...counts),
      min_trades: Math.min(...counts),
      median_trades: this.getMedian(counts),
      average_trades: parseFloat(
        (this.totalTrades / this.signerCounts.size).toFixed(2)
      ),
    };
  }

  // Calculate median
  getMedian(sortedArray) {
    const mid = Math.floor(sortedArray.length / 2);
    return sortedArray.length % 2 !== 0
      ? sortedArray[mid]
      : (sortedArray[mid - 1] + sortedArray[mid]) / 2;
  }

  // Display top N signers
  displayTopSigners(n = 10) {
    const sortedSigners = Array.from(this.signerCounts.entries()).sort(
      (a, b) => b[1] - a[1]
    );

    console.log(`\n=== TOP ${n} MOST ACTIVE SIGNERS ===\n`);

    sortedSigners.slice(0, n).forEach(([signer, count], index) => {
      const percentage = ((count / this.totalTrades) * 100).toFixed(2);
      console.log(`${index + 1}. ${signer}`);
      console.log(`   Trades: ${count} (${percentage}% of total)`);
      console.log("");
    });
  }

  // Main analysis method
  async analyze(csvFilePath, options = {}) {
    try {
      await this.analyzeCSV(csvFilePath);

      // Always show summary for large datasets
      this.displayResults();

      // Save the main CSV output - this is the key deliverable
      const outputFile = this.saveResultsToCSV();

      // Optional: save detailed report
      if (options.saveReport !== false) {
        this.saveDetailedReport();
      }

      return {
        statistics: this.getStatistics(),
        outputFile: outputFile,
        uniqueSigners: this.signerCounts.size,
      };
    } catch (error) {
      console.error("Analysis failed:", error);
      throw error;
    }
  }
}

// Usage function
async function analyzeTradeData(csvFilePath = "matched_trades.csv") {
  const analyzer = new TradeAnalyzer();

  try {
    console.log("🔍 Starting analysis of matched trades...");
    const result = await analyzer.analyze(csvFilePath);

    console.log(`\n✅ Analysis complete!`);
    console.log(`📊 Created aggregated CSV: ${result.outputFile}`);
    console.log(
      `📈 Processed ${result.statistics.total_trades} trades from ${result.uniqueSigners} unique signers`
    );

    return result;
  } catch (error) {
    console.error("❌ Error:", error.message);
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  const csvFile = process.argv[2] || "matched_trades.csv";
  analyzeTradeData(csvFile);
}

// Export for use as module
module.exports = { TradeAnalyzer, analyzeTradeData };
