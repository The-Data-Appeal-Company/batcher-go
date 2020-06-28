package batcher

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestBatcherAccumulateConsumer(t *testing.T) {
	t.Parallel()

	consumer := NewTestConsumer()
	ctx, cancel := context.WithCancel(context.Background())

	batcher := NewBatcher(
		FlushSize(3),
		MaxSize(10),
		FlushTimeout(1*time.Hour),
	)

	res, _ := batcher.StartAsync(ctx, consumer.consumeFn())

	for i := 0; i < 6; i++ {
		batcher.Accumulate(strconv.Itoa(i))
	}

	time.Sleep(300 * time.Millisecond)
	cancel()
	<-res

	assert.Equal(t, consumer.BatchesCount(), 2)
	assert.Equal(t, consumer.TotalElements(), 6)

}

func TestBatcherAccumulateConcurrently(t *testing.T) {
	t.Parallel()

	consumer := NewTestConsumer()
	ctx, cancel := context.WithCancel(context.Background())

	batcher := NewBatcher(
		FlushSize(3),
		MaxSize(10),
		FlushTimeout(1*time.Hour),
	)

	res, _ := batcher.StartAsync(ctx, consumer.consumeFn())

	concurrency := 10

	var wg = &sync.WaitGroup{}
	wg.Add(concurrency)

	for w := 0; w < concurrency; w++ {
		go func(group *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < 6; i++ {
				batcher.Accumulate(strconv.Itoa(i))
			}
		}(wg)
	}

	wg.Wait()
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-res

	assert.Equal(t, consumer.TotalElements(), 6*concurrency)

}

func TestBatcherConsumeConcurrently(t *testing.T) {
	t.Parallel()

	consumer := NewTestConsumer()
	ctx, cancel := context.WithCancel(context.Background())

	batcher := NewBatcher(
		FlushSize(1),
		MaxSize(1000),
		FlushTimeout(1*time.Hour),
		Workers(100),
	)

	res, _ := batcher.StartAsync(ctx, consumer.consumeFn())

	concurrency := 10
	messages := 100

	var wg = &sync.WaitGroup{}
	wg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		go func(group *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < messages; i++ {
				batcher.Accumulate(strconv.Itoa(i))
			}
		}(wg)
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-res

	assert.Equal(t, messages*concurrency, consumer.TotalElements())

}

func TestBatcherConsumeAllWithSlowConsumer(t *testing.T) {
	t.Parallel()

	consumer := NewTestConsumer()
	ctx, cancel := context.WithCancel(context.Background())

	batcher := NewBatcher(
		FlushSize(1),
		MaxSize(1000),
		FlushTimeout(1*time.Hour),
		Workers(20),
	)

	res, _ := batcher.StartAsync(ctx, consumer.consumeFn(func() error {
		time.Sleep(300 * time.Millisecond)
		return nil
	}))

	for i := 0; i < 10; i++ {
		batcher.Accumulate("a")
	}

	time.Sleep(300*time.Millisecond)
	cancel()
	<-res

	assert.Equal(t, 10, consumer.TotalElements())
}

func TestBatcherConsumeAfterCancel(t *testing.T) {
	t.Parallel()

	consumer := NewTestConsumer()
	ctx, cancel := context.WithCancel(context.Background())

	batcher := NewBatcher(
		FlushSize(1000),
		MaxSize(1000),
		FlushTimeout(1*time.Hour),
		Workers(100),
	)

	res, _ := batcher.StartAsync(ctx, consumer.consumeFn())

	concurrency := 10
	messages := 100

	var wg = &sync.WaitGroup{}
	wg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		go func(group *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < messages; i++ {
				batcher.Accumulate(strconv.Itoa(i))
			}
		}(wg)
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-res

	assert.Equal(t, messages*concurrency, consumer.TotalElements())

}

func TestBatcherConsumeWithTimeout(t *testing.T) {
	t.Parallel()

	consumer := NewTestConsumer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewBatcher(
		FlushSize(1000000),
		MaxSize(1000),
		FlushTimeout(20*time.Millisecond),
		Workers(100),
	)

	batcher.StartAsync(ctx, consumer.consumeFn())

	concurrency := 10
	messages := 100

	var wg = &sync.WaitGroup{}
	wg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		go func(group *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < messages; i++ {
				batcher.Accumulate(strconv.Itoa(i))
			}
		}(wg)
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, messages*concurrency, consumer.TotalElements())

}

func TestBatcherCancellation(t *testing.T) {
	t.Parallel()

	consumer := NewTestConsumer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewBatcher(
		FlushSize(1000000),
		MaxSize(1000),
		FlushTimeout(20*time.Millisecond),
		Workers(100),
	)

	concurrency := 10
	messages := 100

	var wg = &sync.WaitGroup{}
	wg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		go func(group *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < messages; i++ {
				batcher.Accumulate(strconv.Itoa(i))
			}
		}(wg)
	}
	wg.Wait()
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := batcher.Start(ctx, consumer.consumeFn())
	assert.NoError(t, err)
	assert.Equal(t, messages*concurrency, consumer.TotalElements())

}

func TestBatcherConsumeError(t *testing.T) {
	t.Parallel()

	consumer := NewTestConsumer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewBatcher(
		FlushSize(1000000),
		MaxSize(1000),
		FlushTimeout(20*time.Millisecond),
		Workers(100),
	)

	concurrency := 10
	messages := 100

	var wg = &sync.WaitGroup{}
	wg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		go func(group *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < messages; i++ {
				batcher.Accumulate(strconv.Itoa(i))
			}
		}(wg)
	}
	wg.Wait()
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := batcher.Start(ctx, consumer.consumeFn(func() error {
		return errors.New("error processing batch")
	}))

	assert.Error(t, err)

}

func BenchmarkBatcherAccumulateConsume(b *testing.B) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewBatcher(
		FlushSize(1000000),
		MaxSize(1000),
		FlushTimeout(20000*time.Millisecond),
		Workers(1),
	)

	done, terminate := batcher.StartAsync(ctx, func(i []interface{}) error {
		return nil
	})

	var data interface{} = "test"

	for i := 0; i < b.N; i++ {
		batcher.Accumulate(data)
		b.ReportAllocs()
		batcher.consumeBatch()
	}

	time.Sleep(100 * time.Millisecond)
	terminate()
	<-done
}

type TestableConsumer struct {
	batches [][]interface{}
	lock    *sync.Mutex
}

func NewTestConsumer() *TestableConsumer {
	return &TestableConsumer{
		batches: make([][]interface{}, 0),
		lock:    &sync.Mutex{},
	}
}

func (t *TestableConsumer) AllBatches() [][]interface{} {
	return t.batches
}

func (t *TestableConsumer) BatchesCount() int {
	return len(t.batches)
}

func (t *TestableConsumer) AllElements() interface{} {
	all := make([]interface{}, 0)
	for _, batch := range t.batches {
		for _, item := range batch {
			all = append(all, item)
		}
	}
	return all
}
func (t *TestableConsumer) TotalElements() int {
	c := 0
	for _, b := range t.batches {
		for range b {
			c++
		}
	}

	return c
}

func (t *TestableConsumer) consumeFn(fns ...func() error) ConsumeBatchFn {
	return func(b []interface{}) error {
		for _, fn := range fns {
			if err := fn(); err != nil {
				return err
			}
		}

		t.lock.Lock()
		t.batches = append(t.batches, b)
		t.lock.Unlock()
		return nil
	}
}
