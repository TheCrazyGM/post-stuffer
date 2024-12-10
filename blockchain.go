package main

import (
	"bytes"
	"encoding/json"
	"net/http"
)

// Block represents a blockchain block
type Block struct {
	BlockNum     string        `json:"block_id"`
	Timestamp    string        `json:"timestamp"`
	Transactions []Transaction `json:"transactions"`
}

// Transaction represents a transaction within a block
type Transaction struct {
	Operations []Operation `json:"operations"`
}

// Operation represents an operation within a transaction
type Operation struct {
	Type  string         `json:"type"`
	Value OperationValue `json:"value"`
}

// OperationValue represents the value of an operation
type OperationValue struct {
	Author       string `json:"author"`
	Title        string `json:"title"`
	Permlink     string `json:"permlink"`
	ParentAuthor string `json:"parent_author"`
	JsonMetadata string `json:"json_metadata"`
}

// Metadata represents the metadata of a post
type Metadata struct {
	Tags []string `json:"tags"`
	App  string   `json:"app"`
}

// getLatestBlock retrieves the latest block number from the Hive blockchain
//
// It makes a request to the Hive API to retrieve the dynamic global properties,
// which contain the latest block number. The function returns the latest block
// number and an error if the request fails.
func getLatestBlock(config *Config) (int, error) {
	resp, err := http.Post(config.HiveAPIURL, "application/json", bytes.NewBuffer([]byte(`{
		"jsonrpc": "2.0",
		"method": "database_api.get_dynamic_global_properties",
		"params": {},
		"id": 1
	}`)))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var result struct {
		Result struct {
			HeadBlockNumber int `json:"head_block_number"`
		} `json:"result"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	return result.Result.HeadBlockNumber, nil
}

// getBlockRange retrieves a range of blocks from the Hive blockchain
//
// It sends a request to the Hive API's block_api.get_block_range method, specifying
// the starting block number and the number of blocks to retrieve. The function
// returns a slice of Block structs and an error if the request or decoding fails.
func getBlockRange(config *Config, startBlock, count int) ([]Block, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "block_api.get_block_range",
		"params": map[string]interface{}{
			"starting_block_num": startBlock,
			"count":              count,
		},
		"id": 1,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(config.HiveAPIURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Result struct {
			Blocks []Block `json:"blocks"`
		} `json:"result"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Result.Blocks, nil
}
