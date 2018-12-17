// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package queue_test

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"storj.io/storj/internal/testcontext"
	"storj.io/storj/pkg/datarepair/queue"
	"storj.io/storj/pkg/pb"
	"storj.io/storj/satellite/satellitedb"
	"storj.io/storj/satellite/satellitedb/satellitedbtest"
	"storj.io/storj/storage/redis"
	"storj.io/storj/storage/redis/redisserver"
	"storj.io/storj/storage/testqueue"
)

func TestRepairQueueDB(t *testing.T) {
	satellitedbtest.Run(t, func(t *testing.T, db *satellitedb.DB) {
		ctx := testcontext.New(t)
		defer ctx.Cleanup()

		testDatabase(ctx, t, db.RepairQueueDB())
	})
}

func testDatabase(ctx context.Context, t *testing.T, q queue.RepairQueue) {
	//testing variables
	seg := pb.InjuredSegment{
		Path:       "this/is/my/path",
		LostPieces: []int32{0,1,2,3,4,5},
	}

	{ // Enqueue
		err := q.Enqueue(ctx, &seg)
		assert.NoError(t, err)

		segs, err := q.Peekqueue(ctx, 1)
		assert.NoError(t, err)
		assert.True(t, proto.Equal(&seg, &segs[0]))
	}

	{ //Dequeue
		entry, err := q.Dequeue(ctx)
		assert.NoError(t, err)
		assert.True(t, proto.Equal(&seg, &entry))

		segs, err := q.Peekqueue(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(segs))
	}
}

func TestEnqueueDequeue(t *testing.T) {
	satellitedbtest.Run(t, func(t *testing.T, db *satellitedb.DB) {
		ctx := testcontext.New(t)
		defer ctx.Cleanup()

		q := db.RepairQueueDB()

		seg := &pb.InjuredSegment{
			Path:       "abc",
			LostPieces: []int32{int32(1), int32(3)},
		}
		err := q.Enqueue(ctx, seg)
		assert.NoError(t, err)
	
		s, err := q.Dequeue(ctx)
		assert.NoError(t, err)
		assert.True(t, proto.Equal(&s, seg))
	})
}

func TestDequeueEmptyQueue(t *testing.T) {
	satellitedbtest.Run(t, func(t *testing.T, db *satellitedb.DB) {
		ctx := testcontext.New(t)
		defer ctx.Cleanup()

		q := db.RepairQueueDB()

		s, err := q.Dequeue(ctx)
		assert.Error(t, err)
		assert.Equal(t, pb.InjuredSegment{}, s)
	})
}

func TestSequential(t *testing.T) {
	satellitedbtest.Run(t, func(t *testing.T, db *satellitedb.DB) {
		ctx := testcontext.New(t)
		defer ctx.Cleanup()

		q := db.RepairQueueDB()
		
		const N = 100
		var addSegs []*pb.InjuredSegment
		for i := 0; i < N; i++ {
			seg := &pb.InjuredSegment{
				Path:       strconv.Itoa(i),
				LostPieces: []int32{int32(i)},
			}
			err := q.Enqueue(ctx, seg)
			assert.NoError(t, err)
			addSegs = append(addSegs, seg)
		}
		list, err := q.Peekqueue(ctx, 100)
		assert.NoError(t, err)
		for i := 0; i < N; i++ {
			assert.True(t, proto.Equal(addSegs[i], &list[i]))
		}
		for i := 0; i < N; i++ {
			dqSeg, err := q.Dequeue(ctx)
			assert.NoError(t, err)
			assert.True(t, proto.Equal(addSegs[i], &dqSeg))
		}
	})
}

func TestParallel(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	queue := queue.NewQueue(testqueue.New())
	const N = 100
	errs := make(chan error, N*2)
	entries := make(chan *pb.InjuredSegment, N*2)
	var wg sync.WaitGroup

	wg.Add(N)
	// Add to queue concurrently
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			err := queue.Enqueue(ctx, &pb.InjuredSegment{
				Path:       strconv.Itoa(i),
				LostPieces: []int32{int32(i)},
			})
			if err != nil {
				errs <- err
			}
		}(i)

	}
	wg.Wait()
	wg.Add(N)
	// Remove from queue concurrently
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			segment, err := queue.Dequeue(ctx)
			if err != nil {
				errs <- err
			}
			entries <- &segment
		}(i)
	}
	wg.Wait()
	close(errs)
	close(entries)

	for err := range errs {
		t.Error(err)
	}

	var items []*pb.InjuredSegment
	for segment := range entries {
		items = append(items, segment)
	}

	sort.Slice(items, func(i, k int) bool { return items[i].LostPieces[0] < items[k].LostPieces[0] })
	// check if the enqueued and dequeued elements match
	for i := 0; i < N; i++ {
		assert.Equal(t, items[i].LostPieces[0], int32(i))
	}
}

func BenchmarkRedisSequential(b *testing.B) {
	addr, cleanup, err := redisserver.Start()
	defer cleanup()
	assert.NoError(b, err)
	client, err := redis.NewQueue(addr, "", 1)
	assert.NoError(b, err)
	q := queue.NewQueue(client)
	benchmarkSequential(b, q)
}

func BenchmarkTeststoreSequential(b *testing.B) {
	q := queue.NewQueue(testqueue.New())
	benchmarkSequential(b, q)
}

func benchmarkSequential(b *testing.B, q queue.RepairQueue) {
	ctx := testcontext.New(b)
	defer ctx.Cleanup()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		const N = 100
		var addSegs []*pb.InjuredSegment
		for i := 0; i < N; i++ {
			seg := &pb.InjuredSegment{
				Path:       strconv.Itoa(i),
				LostPieces: []int32{int32(i)},
			}
			err := q.Enqueue(ctx, seg)
			assert.NoError(b, err)
			addSegs = append(addSegs, seg)
		}
		for i := 0; i < N; i++ {
			dqSeg, err := q.Dequeue(ctx)
			assert.NoError(b, err)
			assert.True(b, proto.Equal(addSegs[i], &dqSeg))
		}
	}
}

func BenchmarkRedisParallel(b *testing.B) {
	addr, cleanup, err := redisserver.Start()
	defer cleanup()
	assert.NoError(b, err)
	client, err := redis.NewQueue(addr, "", 1)
	assert.NoError(b, err)
	q := queue.NewQueue(client)
	benchmarkParallel(b, q)
}

func BenchmarkTeststoreParallel(b *testing.B) {
	q := queue.NewQueue(testqueue.New())
	benchmarkParallel(b, q)
}

func benchmarkParallel(b *testing.B, q queue.RepairQueue) {
	ctx := testcontext.New(b)
	defer ctx.Cleanup()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		const N = 100
		errs := make(chan error, N*2)
		entries := make(chan *pb.InjuredSegment, N*2)
		var wg sync.WaitGroup

		wg.Add(N)
		// Add to queue concurrently
		for i := 0; i < N; i++ {
			go func(i int) {
				defer wg.Done()
				err := q.Enqueue(ctx, &pb.InjuredSegment{
					Path:       strconv.Itoa(i),
					LostPieces: []int32{int32(i)},
				})
				if err != nil {
					errs <- err
				}
			}(i)

		}
		wg.Wait()
		wg.Add(N)
		// Remove from queue concurrently
		for i := 0; i < N; i++ {
			go func(i int) {
				defer wg.Done()
				segment, err := q.Dequeue(ctx)
				if err != nil {
					errs <- err
				}
				entries <- &segment
			}(i)
		}
		wg.Wait()
		close(errs)
		close(entries)

		for err := range errs {
			b.Error(err)
		}

		var items []*pb.InjuredSegment
		for segment := range entries {
			items = append(items, segment)
		}

		sort.Slice(items, func(i, k int) bool { return items[i].LostPieces[0] < items[k].LostPieces[0] })
		// check if the enqueued and dequeued elements match
		for i := 0; i < N; i++ {
			assert.Equal(b, items[i].LostPieces[0], int32(i))
		}
	}
}
