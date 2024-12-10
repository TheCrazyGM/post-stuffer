package main

import (
	"fmt"
	"log"
	"time"
)

// retryWithBackoff executes the given function with exponential backoff. It
// retries the given operation up to maxRetries times, with an initial delay of
// retryDelay and a backoff factor of 2. If all retries fail, it returns the last
// error encountered.
func retryWithBackoff(maxRetries int, retryDelay time.Duration, operation func() error) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if err := operation(); err != nil {
			lastErr = err
			delay := retryDelay * time.Duration(1<<uint(i))
			log.Printf("Attempt %d/%d failed: %v. Retrying in %v...", i+1, maxRetries, err, delay)
			time.Sleep(delay)
			continue
		}
		return nil
	}
	return fmt.Errorf("operation failed after %d attempts. Last error: %v", maxRetries, lastErr)
}

// constructAuthorPerm creates a string in the format "@author/permlink"
func constructAuthorPerm(author, permlink string) string {
	return fmt.Sprintf("@%s/%s", author, permlink)
}
