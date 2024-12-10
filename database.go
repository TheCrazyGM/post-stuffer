package main

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// initDB initializes the SQLite database and creates the "posts" table if it doesn't
// exist. The table has the following columns:
//
//   - _id: an autoincrementing unique identifier
//   - url: a unique string identifier for the post
//   - author: the author of the post
//   - permlink: the permlink of the post
//   - title: the title of the post
//   - json_metadata: the JSON metadata of the post
//   - block_num: the block number that the post was published in
//   - timestamp: the timestamp of the post
//
// Additionally, the function creates two indexes on the table, one on the block_num
// field and one on the author field.
func initDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "blocks.db")
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}

	// Create the posts table if it doesn't exist
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS posts (
		_id INTEGER PRIMARY KEY AUTOINCREMENT,
		url TEXT UNIQUE,
		author TEXT,
		permlink TEXT,
		title TEXT,
		json_metadata TEXT,
		block_num INTEGER,
		timestamp TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_block_num ON posts(block_num);
	CREATE INDEX IF NOT EXISTS idx_author ON posts(author);
	`

	if _, err := db.Exec(createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("error creating table: %v", err)
	}

	return db, nil
}

// getLastProcessedBlock retrieves the last processed block number from the database.
//
// If the database is empty, it returns the genesis block number.
//
// Args:
//
//	db: the database connection
//	genesisBlock: the genesis block number
//
// Returns:
//
//	the last processed block number
//	an error if there is an issue with the database query
func getLastProcessedBlock(db *sql.DB, genesisBlock int) (int, error) {
	var blockNum sql.NullInt64
	err := db.QueryRow("SELECT MAX(block_num) FROM posts").Scan(&blockNum)
	if err != nil {
		return 0, err
	}

	if !blockNum.Valid {
		return genesisBlock, nil
	}

	return int(blockNum.Int64), nil
}
