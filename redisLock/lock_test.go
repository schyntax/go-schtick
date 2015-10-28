package redisLock

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/schyntax/go-schtick"
)

var pool = &redis.Pool{
	MaxIdle:     3,
	IdleTimeout: 240 * time.Second,
	Dial: func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", "localhost:6379", redis.DialDatabase(2))
		if err != nil {
			return nil, err
		}
		return c, err
	},
}

var count = new(int64)

func TestMultiple(t *testing.T) {
	locker := New(pool.Get)

	//try to make sure we are exactly halfway between seconds when we start
	offset := time.Now().Sub(time.Now().Truncate(time.Second))
	time.Sleep(time.Second - offset + (500 * time.Millisecond))

	sch := schtick.New(nil)
	sch.AddTaskWithDefaults("taskA", "s(*)", locker.Wrap(run))

	sch2 := schtick.New(nil)
	sch2.AddTaskWithDefaults("taskA", "s(*)", locker.Wrap(run))

	time.Sleep(3 * time.Second)

	if *count != 3 {
		t.Fatalf("Expected exactly 3 events, but received %d.", *count)
	}

}

func run(task schtick.Task, timeIntendedToRun time.Time) error {
	atomic.AddInt64(count, 1)
	return nil
}
