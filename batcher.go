package batcher

import (
	"context"
	"golang.org/x/sync/errgroup"
	"sync"
	"time"
)

type Conf struct {
	maxSize      int
	flushSize    int
	flushTimeout time.Duration
	workers      int
}

type ConsumeBatchFn func([]interface{}) error

type Batcher struct {
	dataCh    chan interface{}
	batch     []interface{}
	batchLock *sync.Mutex
	conf      Conf
	batchCh   chan []interface{}
	outputCh  chan error
}

func NewBatcher(opts ...Opt) *Batcher {
	conf := &Conf{
		maxSize:      10,
		flushSize:    10,
		flushTimeout: 1 * time.Second,
		workers:      1,
	}

	for _, opt := range opts {
		opt.Apply(conf)
	}

	return &Batcher{
		dataCh:    make(chan interface{}, conf.maxSize),
		batchCh:   make(chan []interface{}, conf.workers),
		batch:     make([]interface{}, 0),
		batchLock: &sync.Mutex{},
		conf:      *conf,
	}
}

func (b *Batcher) Accumulate(data interface{}) {
	b.dataCh <- data
}

func (b *Batcher) StartAsync(ctx context.Context, fn ConsumeBatchFn) (chan error, func()) {
	ctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 0)
	go func() {
		errCh <- b.Start(ctx, fn)
	}()

	return errCh, cancel
}

func (b *Batcher) Start(ctx context.Context, fn ConsumeBatchFn) error {
	g, gctx := errgroup.WithContext(ctx)

	var wg = &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		after := time.After(b.conf.flushTimeout)

		for {
			select {
			case item, open := <-b.dataCh:
				if !open {
					return
				}

				// Should block only if we are copying the batch for consumption
				b.batchLock.Lock()
				b.batch = append(b.batch, item)
				b.batchLock.Unlock()

				if len(b.batch) >= b.conf.flushSize {
					b.consumeBatch()
				}
				after = time.After(b.conf.flushTimeout)
			case <-after:
				b.consumeBatch()

			case <-ctx.Done():
				b.consumeBatch()
				close(b.batchCh)
				return
			}
		}
	}()

	for i := 0; i < b.conf.workers; i++ {
		g.Go(func() error {
			for {
				select {
				case batch := <-b.batchCh:
					err := fn(batch)
					if err != nil {
						return err
					}

				case <-gctx.Done():
					// when terminated process pending batches
					for b := range b.batchCh {
						if err := fn(b); err != nil {
							return err
						}
					}
					return nil
				}
			}
		})
	}

	err := g.Wait()
	close(b.dataCh)
	wg.Wait()

	return err
}

func (b *Batcher) consumeBatch() {
	// fast count without locking
	if len(b.batch) == 0 {
		return
	}

	// lock and clone batch
	b.batchLock.Lock()
	batch := make([]interface{}, len(b.batch))
	copy(batch, b.batch)

	b.batch = b.batch[:0]
	b.batchLock.Unlock()
	// once we copied all the data we can unlock the writers

	b.batchCh <- batch
}
