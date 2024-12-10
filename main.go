package main

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"
)

func main() {
	// Initialize configuration
	config := DefaultConfig()

	// Initialize database with retry
	var db *sql.DB
	err := retryWithBackoff(config.MaxRetries, config.RetryDelay, func() error {
		var err error
		db, err = initDB()
		return err
	})
	if err != nil {
		log.Fatal("Error initializing database:", err)
	}
	defer db.Close()

	// Create block processor
	processor, err := NewBlockProcessor(db, config)
	if err != nil {
		log.Fatal("Error creating block processor:", err)
	}
	defer processor.Close()

	// Get current block and last processed block with retry
	var currentBlock, lastProcessed int
	err = retryWithBackoff(config.MaxRetries, config.RetryDelay, func() error {
		var err error
		currentBlock, err = getLatestBlock(config)
		if err != nil {
			return fmt.Errorf("error getting latest block: %v", err)
		}

		lastProcessed, err = getLastProcessedBlock(db, config.GenesisBlock)
		if err != nil {
			return fmt.Errorf("error getting last processed block: %v", err)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Initialize lastProcessed to genesisBlock if it is 0
	if lastProcessed == 0 {
		lastProcessed = config.GenesisBlock
	}

	// Calculate initial variance
	variance := currentBlock - lastProcessed
	log.Printf("Starting block processing - Current: %d, Last: %d, Variance: %d\n",
		currentBlock, lastProcessed, variance)

	// Process blocks in batches
	startTime := time.Now()
	totalProcessed := 0
	totalInserts := 0

	for variance > 0 {
		startBlock := lastProcessed + 1
		count := config.BatchSize
		if startBlock+count > currentBlock {
			count = currentBlock - startBlock + 1
		}

		// Fetch blocks with retry
		var blocks []Block
		err := retryWithBackoff(config.MaxRetries, config.RetryDelay, func() error {
			var err error
			blocks, err = getBlockRange(config, startBlock, count)
			return err
		})
		if err != nil {
			log.Printf("Error getting blocks: %v\n", err)
			continue
		}

		batchStartTime := time.Now()
		batchInserts := 0

		for _, block := range blocks {
			if block.BlockNum == "0" {
				continue
			}

			insertCount, err := processor.processBlock(block)
			if err != nil {
				log.Printf("Error processing block %s: %v\n", block.BlockNum, err)
				continue
			}

			// Update progress tracking
			hexBlockNum := block.BlockNum[:8]
			blockNum, _ := strconv.ParseInt(hexBlockNum, 16, 32)
			lastProcessed = int(blockNum)
			batchInserts += insertCount
			totalProcessed++
		}

		totalInserts += batchInserts
		batchDuration := time.Since(batchStartTime)
		totalDuration := time.Since(startTime)
		percentage := float64(lastProcessed-config.GenesisBlock) / float64(currentBlock-config.GenesisBlock) * 100

		// Log progress with detailed statistics
		log.Printf("Progress: %.2f%% | Block: %d | Batch: %d blocks, %d posts in %.2fs (%.1f blocks/s) | Total: %d blocks, %d posts in %.0fs\n",
			percentage, startBlock, len(blocks), batchInserts, batchDuration.Seconds(),
			float64(len(blocks))/batchDuration.Seconds(),
			totalProcessed, totalInserts, totalDuration.Seconds())

		// Recalculate variance
		variance = currentBlock - lastProcessed
	}

	log.Printf("Processing complete - Total blocks: %d, Total posts: %d, Time: %.0fs\n",
		totalProcessed, totalInserts, time.Since(startTime).Seconds())
}
