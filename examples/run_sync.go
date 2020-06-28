package examples

import (
	"context"
	"github.com/GaruGaru/batcher-go"
	"time"
)

func exampleRunSync() {
	batcher := batcher.NewBatcher(
		batcher.FlushSize(10),                      // Flush every 10 messages
		batcher.MaxSize(1000),                      // After 1000 messages blocks the producer
		batcher.FlushTimeout(200*time.Millisecond), // if flush size is not reached flush anyway after 200 ms
		batcher.Workers(10),                        // Number of batch processors
	)

	batcher.Accumulate("a")
	batcher.Accumulate("b")
	batcher.Accumulate("c")

	// Start fn will block until context is done or an error is raised by the consumer
	err := batcher.Start(context.Background(), func(batch []interface{}) error {
		// process batch
		return nil
	})

	if err != nil {
		panic(err)
	}
}
