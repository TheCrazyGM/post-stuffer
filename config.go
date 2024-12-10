package main

import "time"

// Config holds the application configuration
type Config struct {
	HiveAPIURL   string
	GenesisBlock int
	BatchSize    int
	DBPath       string
	MaxRetries   int
	RetryDelay   time.Duration
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		HiveAPIURL:   "https://api.hive.blog",
		GenesisBlock: 41818753,
		BatchSize:    1000,
		DBPath:       "blocks.db",
		MaxRetries:   3,
		RetryDelay:   time.Second * 2,
	}
}
