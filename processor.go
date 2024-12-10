package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
)

// BlockProcessor handles the processing of blockchain blocks
type BlockProcessor struct {
	db     *sql.DB
	config *Config
	stmt   *sql.Stmt
}

// NewBlockProcessor creates a new BlockProcessor instance
//
// The BlockProcessor instance will be connected to the given database and configured
// with the given configuration.
//
// The prepared statement is created here to avoid creating a new prepared statement
// for each block processed.
//
// The ON CONFLICT(url) DO NOTHING statement means that if a post with the same URL
// already exists in the database, this statement will not overwrite it.
func NewBlockProcessor(db *sql.DB, config *Config) (*BlockProcessor, error) {
	stmt, err := db.Prepare(`
		INSERT INTO posts (url, author, permlink, title, tags, block_num, timestamp)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(url) DO NOTHING
	`)
	if err != nil {
		return nil, fmt.Errorf("error preparing statement: %v", err)
	}

	return &BlockProcessor{
		db:     db,
		config: config,
		stmt:   stmt,
	}, nil
}

// Close releases resources held by the BlockProcessor
//
// This function should be called when the BlockProcessor is no longer needed
// to release the resources held by the prepared statement.
func (bp *BlockProcessor) Close() error {
	if bp.stmt != nil {
		return bp.stmt.Close()
	}
	return nil
}

// processBlock processes a single block and stores relevant post information in the database.
//
// It iterates over the transactions and operations within the block, filtering for
// "comment_operation" types. It skips comments that are replies (i.e., have a parent author).
// For each valid operation, it attempts to parse the JSON metadata, handling malformed
// metadata by using a fallback structure. The post information is then inserted into the
// database using a prepared statement, with retries applied in case of failure.
//
// Returns the number of processed posts and an error if any database operation fails.
func (bp *BlockProcessor) processBlock(block Block) (int, error) {
	// Take first 8 characters of block ID and parse as hex
	hexBlockNum := block.BlockNum[:8]
	blockNum, err := strconv.ParseInt(hexBlockNum, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("error converting block number from hex: %v", err)
	}

	var processedCount int
	for _, tx := range block.Transactions {
		for _, op := range tx.Operations {
			// log.Printf("Processing operation type: %s", op.Type) // Log operation type
			if op.Type != "comment_operation" {
				continue
			}

			value := op.Value
			// log.Printf("Operation value: %+v", value) // Log operation value
			if value.ParentAuthor != "" {
				continue // Skip comments/replies
			}

			var metadata struct {
				Tags interface{} `json:"tags"`
			}
			var tagsJson string
			if value.JsonMetadata != "" {
				if err := json.Unmarshal([]byte(value.JsonMetadata), &metadata); err != nil {
					// If parsing fails, try to handle it as a single tag string
					metadata.Tags = value.JsonMetadata
				}

				// Convert tags to JSON string based on type
				switch v := metadata.Tags.(type) {
				case string:
					// If it's a single string, create a JSON array with one element
					tagsJson = fmt.Sprintf("[%q]", v)
				case []interface{}:
					// If it's already an array, convert it to JSON
					tagsBytes, err := json.Marshal(v)
					if err == nil {
						tagsJson = string(tagsBytes)
					} else {
						tagsJson = "[]"
					}
				default:
					tagsJson = "[]"
				}
			} else {
				tagsJson = "[]"
			}

			// Retry the database operation with backoff
			err = retryWithBackoff(bp.config.MaxRetries, bp.config.RetryDelay, func() error {
				_, err := bp.stmt.Exec(
					constructAuthorPerm(value.Author, value.Permlink),
					value.Author,
					value.Permlink,
					value.Title,
					tagsJson,
					int(blockNum),
					block.Timestamp,
				)
				return err
			})
			if err != nil {
				return processedCount, fmt.Errorf("error inserting post: %v", err)
			}

			processedCount++
		}
	}

	return processedCount, nil
}
