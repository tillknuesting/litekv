package litekv

import (
	"sync"
	"time"
)

// A store takes one write at a time, and two goroutines writing do not merely
// fail to go faster: they halve the throughput of the store, because every
// write takes every shard of the lock and the second writer spends its time
// waiting for the first. A server with a handler per request walks straight
// into that, since the handlers are the goroutines.
//
// A Writer is the answer, and it is the ordinary one: the callers hand their
// records to a queue, one goroutine writes, and nothing contends on anything.
// What makes it worth more than that is the batch — everything waiting when the
// writer wakes goes down as one, so a hundred callers cost one write to the log
// and one sync between them instead of a hundred of each. That is group commit,
// and under SyncAlways it is the difference between a store that does two
// hundred writes a second and one that does as many as fit in a batch.
//
// The queue does not make writing faster for one caller. It makes concurrent
// callers stop taking it away from each other, and it is much easier to put in
// before there is an API in front of it than afterwards.

// Writer serializes writes to a store through one goroutine, and stores
// everything waiting at once.
//
// Its methods block until the records are stored, exactly as the store's own do
// — including the wait for the disk that the sync policy asks for — so a caller
// may reuse its key and value slices as soon as one returns, and a caller that
// is told a record is stored has the same promise it always had.
//
// A Writer is safe for concurrent use, which is the whole point of it. Close it
// when the callers are done.
type Writer struct {
	store interface {
		WriteBatch(*Batch) error
	}

	// mu is held for reading while a record is put on the queue and for
	// writing while the queue is closed, which is what keeps a send from
	// racing the close. Readers do not contend with each other, so the cost on
	// the way in is a channel send and nothing else.
	mu     sync.RWMutex
	closed bool

	queue chan *waiting
	done  chan struct{}
}

// WriterOptions configures a Writer. The zero value queues a thousand records.
type WriterOptions struct {
	// Queue is how many callers may be waiting to be written before another one
	// blocks on the way in. Zero means 1024.
	//
	// It bounds a group as well as the wait: the writer takes everything queued
	// when it wakes, so a deeper queue is a larger batch under load and a
	// larger amount of memory held while it is written.
	Queue int
}

const defaultQueue = 1024

func (o WriterOptions) queue() int {
	if o.Queue <= 0 {
		return defaultQueue
	}
	return o.Queue
}

// waiting is one caller's records and the answer it is waiting for. The single
// record is held by value, so the ordinary write puts nothing on the heap that
// the pool does not already have.
type waiting struct {
	one     Record
	records []Record
	result  chan error
}

var waitingPool = sync.Pool{
	New: func() any { return &waiting{result: make(chan error, 1)} },
}

// Writer returns a Writer that stores this store's records from one goroutine.
// Close it when the callers are done with it.
func (kvs *KeyValueStore) Writer(opts WriterOptions) *Writer {
	return newWriter(kvs, opts)
}

// Writer returns a Writer that stores this store's records from one goroutine,
// which is what a server with a handler per request wants in front of it.
// Close it when the callers are done with it.
func (db *DB) Writer(opts WriterOptions) *Writer {
	return newWriter(db, opts)
}

func newWriter(store interface{ WriteBatch(*Batch) error }, opts WriterOptions) *Writer {
	w := &Writer{
		store: store,
		queue: make(chan *waiting, opts.queue()),
		done:  make(chan struct{}),
	}

	go w.run()
	return w
}

// Write stores the record and returns once it is stored, on the same terms as
// the store's own Write.
func (w *Writer) Write(key, value []byte) error {
	item := waitingPool.Get().(*waiting)
	item.one = Record{
		Type:        RecordTypeNormal,
		Key:         key,
		Value:       value,
		KeyLength:   uint32(len(key)),
		ValueLength: uint32(len(value)),
	}
	return w.submit(item)
}

// WriteExpiring stores a record that stops counting once at has passed, on the
// same terms as the store's own WriteExpiring.
func (w *Writer) WriteExpiring(key, value []byte, at time.Time) error {
	expires := int64(0)
	if !at.IsZero() {
		expires = at.UnixNano()
	}

	item := waitingPool.Get().(*waiting)
	item.one = Record{
		Type:        RecordTypeNormal,
		Expires:     expires,
		Key:         key,
		Value:       value,
		KeyLength:   uint32(len(key)),
		ValueLength: uint32(len(value)),
	}
	return w.submit(item)
}

// Delete stores a tombstone for the key, on the same terms as the store's own
// Delete.
func (w *Writer) Delete(key []byte) error {
	item := waitingPool.Get().(*waiting)
	item.one = Record{
		Type:      RecordTypeDeleted,
		Key:       key,
		KeyLength: uint32(len(key)),
	}
	return w.submit(item)
}

// WriteBatch stores every record in b, or none of them.
//
// The batch keeps its atomicity: its records go into the group in the order
// they were added and with nothing of anybody else's between them, and the
// group as a whole is one batch, so what a crash can lose is the group rather
// than part of anyone's batch. The keys and values are read while this waits,
// so they have to stay unchanged until it returns.
func (w *Writer) WriteBatch(b *Batch) error {
	if b == nil || len(b.entries) == 0 {
		return nil
	}

	item := waitingPool.Get().(*waiting)
	item.records = b.entries
	return w.submit(item)
}

// submit puts one caller's records on the queue and waits for the answer.
func (w *Writer) submit(item *waiting) error {
	defer func() {
		item.one, item.records = Record{}, nil
		waitingPool.Put(item)
	}()

	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return ErrorClosed
	}
	w.queue <- item
	w.mu.RUnlock()

	// Whatever is on the queue is answered, including by a writer on its way
	// out, so this cannot be left waiting by a Close.
	return <-item.result
}

// run is the one goroutine that writes. It takes the first caller off the
// queue, sweeps up everybody else who is already waiting, and stores the lot as
// one batch.
//
// The sweep is what makes this worth having. Under a sync policy that waits for
// the disk, the wait is per write to the log rather than per record, so a group
// of a hundred pays for one — and the busier the store is, the more callers are
// waiting when the writer wakes, which is the right way round for a queue to
// behave under load.
func (w *Writer) run() {
	defer close(w.done)

	var group Batch
	var waiters []*waiting

	for first := range w.queue {
		group.Reset()
		waiters = waiters[:0]

		take := func(item *waiting) {
			waiters = append(waiters, item)
			if item.records != nil {
				group.entries = append(group.entries, item.records...)
				return
			}
			group.entries = append(group.entries, item.one)
		}

		take(first)

		// Everybody who is already waiting, and nobody who is not: this never
		// waits for a group to fill, so a store nobody else is writing to is
		// one write behind one caller and no slower than it ever was.
		sweeping := true
		for sweeping {
			select {
			case next, ok := <-w.queue:
				if !ok {
					sweeping = false
					break
				}
				take(next)
			default:
				sweeping = false
			}
		}

		err := w.store.WriteBatch(&group)
		for _, item := range waiters {
			item.result <- err
		}
	}
}

// Close stops the Writer, once everything already queued has been written.
//
// A caller waiting for a record to be stored is not abandoned: the records on
// the queue are written and answered before the goroutine goes. Anything that
// arrives afterwards reports ErrorClosed. Closing twice is harmless, and this
// does not close the store — that is the caller's, and it should be closed
// after this.
func (w *Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	close(w.queue)
	w.mu.Unlock()

	<-w.done
	return nil
}
