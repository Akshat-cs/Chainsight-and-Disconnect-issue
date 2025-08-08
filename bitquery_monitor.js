const WebSocket = require("ws");
const fs = require("fs");
const csv = require("csv-parser");
require("dotenv").config();

class BitQueryTradeMonitor {
  constructor() {
    this.ws = null;
    this.addressSet = new Set();
    this.matchedTrades = [];
    this.startTime = null;
    this.duration = 30 * 60 * 1000;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.reconnectDelay = 3000; // 3 seconds
    this.subscriptionId = "dex-trades-subscription"; // Unique ID for the subscription

    // Silent disconnect detection
    this.lastDataReceived = null;
    this.silentDisconnectTimeout = 20000; // 20 seconds without data = silent disconnect
    this.silentDisconnectTimer = null;
    this.heartbeatInterval = null;
  }

  // Load addresses from CSV file into a Set for O(1) lookup
  async loadAddresses(csvFilePath) {
    return new Promise((resolve, reject) => {
      console.log("Loading addresses from CSV...");
      let count = 0;

      fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on("data", (row) => {
          // Assuming the CSV has a column named 'address'
          if (row.address) {
            this.addressSet.add(row.address.trim());
            count++;
          }
        })
        .on("end", () => {
          console.log(`Loaded ${count} addresses into memory`);
          resolve();
        })
        .on("error", (error) => {
          console.error("Error reading CSV:", error);
          reject(error);
        });
    });
  }

  // Create WebSocket connection to BitQuery
  connect() {
    const token = process.env.BITQUERY_API_KEY;
    if (!token) {
      console.error("BITQUERY_API_KEY not found in environment variables");
      process.exit(1);
    }

    // Use EAP endpoint with token as query parameter and graphql-ws protocol
    const bitqueryUrl = `wss://streaming.bitquery.io/eap?token=${token}`;

    this.ws = new WebSocket(bitqueryUrl, ["graphql-ws"]);

    this.ws.on("open", () => {
      console.log("Connected to BitQuery WebSocket");
      this.reconnectAttempts = 0;

      // Start silent disconnect detection
      this.startSilentDisconnectDetection();

      // Send connection_init message first
      const initMessage = JSON.stringify({ type: "connection_init" });
      this.ws.send(initMessage);
      console.log("Connection init message sent");
    });

    this.ws.on("message", (data) => {
      // Make message handling async to avoid blocking the WebSocket
      this.handleMessage(data).catch((error) => {
        console.error("Error in handleMessage:", error);
      });
    });

    this.ws.on("close", (code, reason) => {
      console.log(`WebSocket closed: ${code} - ${reason}`);
      this.stopSilentDisconnectDetection();
      this.handleReconnect();
    });

    this.ws.on("error", (error) => {
      console.error("WebSocket error:", error);
      this.stopSilentDisconnectDetection();
    });
  }

  // Send subscription query to BitQuery (only called after connection_ack)
  startSubscription() {
    const subscription = {
      id: this.subscriptionId,
      type: "start",
      payload: {
        query: `
                    subscription MyQuery {
                        Solana {
                            DEXTrades(where: {Transaction: {Result: {Success: true}}}) {
                                Instruction {
                                    Program {
                                        Method
                                    }
                                }
                                Block {
                                    Time
                                }
                                Trade {
                                    Buy {
                                        Amount
                                        Account {
                                            Address
                                        }
                                        Currency {
                                            Name
                                            Symbol
                                            MintAddress
                                            Decimals
                                        }
                                        AmountInUSD
                                    }
                                    Sell {
                                        Amount
                                        Account {
                                            Address
                                        }
                                        Currency {
                                            Name
                                            Symbol
                                            MintAddress
                                            Decimals
                                        }
                                        AmountInUSD
                                    }
                                }
                                Transaction {
                                    Signature
                                    Signer
                                }
                            }
                        }
                    }
                `,
      },
    };

    this.ws.send(JSON.stringify(subscription));
    console.log("Subscription started with ID:", this.subscriptionId);

    // Set start time and schedule disconnect only after successful subscription
    if (!this.startTime) {
      this.startTime = Date.now();
      this.scheduleDisconnect();
    }
  }

  // Handle incoming messages (async to prevent blocking WebSocket)
  async handleMessage(data) {
    try {
      const response = JSON.parse(data);

      // Update last data received timestamp for any message
      this.lastDataReceived = Date.now();
      this.resetSilentDisconnectTimer();

      // Handle connection acknowledgment
      if (response.type === "connection_ack") {
        console.log("Connection acknowledged by server");
        this.startSubscription();
      }

      // Handle received data
      if (
        response.type === "data" &&
        response.payload?.data?.Solana?.DEXTrades
      ) {
        const trades = response.payload.data.Solana.DEXTrades;
        // Process trades asynchronously to avoid blocking
        await this.processTrades(trades);
      }

      // Handle keep-alive messages
      if (response.type === "ka") {
        // Keep-alive received - connection is healthy
      }

      // Handle errors
      if (response.type === "error") {
        console.error("Error message received:", response.payload.errors);
      }
    } catch (error) {
      console.error("Error parsing message:", error);
    }
  }

  // Process incoming trades and filter by address list (async to prevent blocking)
  async processTrades(trades) {
    const matchPromises = [];

    for (const trade of trades) {
      const signer = trade.Transaction?.Signer;

      // Check if the transaction signer is in our address list
      if (signer && this.addressSet.has(signer)) {
        const matchedTrade = {
          signature: trade.Transaction.Signature,
          signer: trade.Transaction.Signer,
          blockTime: trade.Block.Time,
        };

        this.matchedTrades.push(matchedTrade);

        // Save asynchronously to file without blocking
        matchPromises.push(this.saveTradeToFile(matchedTrade));
      }
    }

    // Process all file writes concurrently
    if (matchPromises.length > 0) {
      await Promise.all(matchPromises);
    }
  }

  // Save individual trade to file (async to prevent blocking)
  async saveTradeToFile(trade) {
    return new Promise((resolve, reject) => {
      const csvLine = `${trade.signature},${trade.signer},${trade.blockTime}\n`;

      fs.appendFile("matched_trades.csv", csvLine, (err) => {
        if (err) {
          console.error("Error writing to file:", err);
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  // Schedule disconnect after 1 hour
  scheduleDisconnect() {
    setTimeout(() => {
      this.gracefulDisconnect();
    }, this.duration);

    console.log(`Scheduled disconnect in ${this.duration / 60000} minutes`);
  }

  // Start silent disconnect detection
  startSilentDisconnectDetection() {
    this.lastDataReceived = Date.now();
    this.resetSilentDisconnectTimer();
    console.log("Silent disconnect detection started");
  }

  // Stop silent disconnect detection
  stopSilentDisconnectDetection() {
    if (this.silentDisconnectTimer) {
      clearTimeout(this.silentDisconnectTimer);
      this.silentDisconnectTimer = null;
    }
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  // Reset the silent disconnect timer
  resetSilentDisconnectTimer() {
    if (this.silentDisconnectTimer) {
      clearTimeout(this.silentDisconnectTimer);
    }

    this.silentDisconnectTimer = setTimeout(() => {
      const timeSinceLastData = Date.now() - this.lastDataReceived;
      console.log(
        `Silent disconnect detected! No data received for ${timeSinceLastData}ms`
      );
      console.log("Forcing reconnection in 3 seconds...");

      // Close current connection and reconnect after 3 seconds
      if (this.ws) {
        this.ws.close();
      }

      setTimeout(() => {
        console.log("Attempting reconnection after silent disconnect...");
        this.handleReconnect();
      }, 3000); // 3 seconds as specified by Divyashree
    }, this.silentDisconnectTimeout);
  }

  // Handle reconnection logic
  handleReconnect() {
    this.stopSilentDisconnectDetection();

    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      console.log(
        `Attempting to reconnect (${this.reconnectAttempts}/${this.maxReconnectAttempts})...`
      );

      setTimeout(() => {
        this.connect();
      }, this.reconnectDelay);
    } else {
      console.log("Max reconnection attempts reached. Exiting...");
      this.gracefulDisconnect();
    }
  }

  // Graceful disconnect
  async gracefulDisconnect() {
    console.log("Initiating graceful disconnect...");

    // Stop silent disconnect detection
    this.stopSilentDisconnectDetection();

    if (this.ws) {
      this.ws.close(1000, "Normal closure");
    }

    // Save final summary
    this.saveSummary();

    console.log(
      `Monitoring completed. Found ${this.matchedTrades.length} matching trades.`
    );
    process.exit(0);
  }

  // Save summary of the session
  saveSummary() {
    const summary = {
      startTime: new Date(this.startTime).toISOString(),
      endTime: new Date().toISOString(),
      duration: Date.now() - this.startTime,
      totalMatchedTrades: this.matchedTrades.length,
      addressesMonitored: this.addressSet.size,
    };

    fs.writeFileSync("session_summary.json", JSON.stringify(summary, null, 2));
    console.log("Session summary saved to session_summary.json");
  }

  // Initialize CSV file with headers
  initializeOutputFile() {
    const headers = "signature,signer,blockTime\n";
    fs.writeFileSync("matched_trades.csv", headers);
    console.log("Output file initialized: matched_trades.csv");
  }

  // Main method to start monitoring
  async start(csvFilePath) {
    try {
      await this.loadAddresses(csvFilePath);
      this.initializeOutputFile();
      this.connect();

      // Handle process termination gracefully
      process.on("SIGINT", () => {
        console.log("\nReceived SIGINT. Shutting down gracefully...");
        this.gracefulDisconnect();
      });

      process.on("SIGTERM", () => {
        console.log("\nReceived SIGTERM. Shutting down gracefully...");
        this.gracefulDisconnect();
      });
    } catch (error) {
      console.error("Error starting monitor:", error);
      process.exit(1);
    }
  }
}

// Usage
const monitor = new BitQueryTradeMonitor();
monitor.start("address_list_no_filter.csv");

// Export for use as module
module.exports = BitQueryTradeMonitor;
