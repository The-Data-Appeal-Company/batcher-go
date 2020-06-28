package batcher

import "time"

type Opt interface {
	Apply(c *Conf)
}

type MaxSizeOpt struct {
	max int
}

func (m MaxSizeOpt) Apply(c *Conf) {
	c.maxSize = m.max
}

func MaxSize(maxSize int) Opt {
	return MaxSizeOpt{max: maxSize}
}

type FlushSizeOpt struct {
	flush int
}

func (m FlushSizeOpt) Apply(c *Conf) {
	c.flushSize = m.flush
}

func FlushSize(flushSize int) Opt {
	return FlushSizeOpt{flush: flushSize}
}

type FlushTimeoutOpt struct {
	timeout time.Duration
}

func (m FlushTimeoutOpt) Apply(c *Conf) {
	c.flushTimeout = m.timeout
}

func FlushTimeout(flushTimeout time.Duration) Opt {
	return FlushTimeoutOpt{timeout: flushTimeout}
}

type WorkersOpt struct {
	workers int
}

func (m WorkersOpt) Apply(c *Conf) {
	c.workers = m.workers
}

func Workers(workers int) Opt {
	return WorkersOpt{workers: workers}
}
