// package residLock provides a way to use redis to lock tasks before running them.
// This will ensure that only one server runs a task in any given interval.
// Fully compatible with other language implementations.
package redisLock

import (
	"fmt"
	"os"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/schyntax/go-schtick"
)

type Locker interface {
	Wrap(schtick.TaskCallback) schtick.TaskCallback

	SetHostname(string)
	SetPrefix(string)
}

type taskLocker struct {
	connect func() redis.Conn
	host    string
	prefix  string
	lastKey string
}

func (t *taskLocker) SetHostname(n string) {
	t.host = n
}
func (t *taskLocker) SetPrefix(p string) {
	t.prefix = p
	t.lastKey = p + "_last"
}

func New(connect func() redis.Conn) Locker {
	host, err := os.Hostname()
	if err != nil {
		host = "???"
	}
	locker := &taskLocker{
		connect: connect,
		host:    host,
	}
	locker.SetPrefix("schyntax")
	return locker
}

const LOCK_SCRIPT = `
if redis.call('set', KEYS[1], KEYS[2], 'nx', 'px', KEYS[3])
then
    redis.call('hset', KEYS[4], KEYS[5], KEYS[6])
    return 1
else
    return 0
end
`

var script = redis.NewScript(6, LOCK_SCRIPT)

func (t *taskLocker) Wrap(cb schtick.TaskCallback) schtick.TaskCallback {
	return func(task schtick.Task, timeIntendedToRun time.Time) error {
		const format = "2006-01-02T15:04:05.0000000-07:00"
		iso := timeIntendedToRun.Format(format)
		lockKey := fmt.Sprintf("%s;%s;%s", t.prefix, task.Name(), iso)
		lastLockValue := fmt.Sprintf("%s;%s;%s", iso, time.Now().UTC().Format(format), t.host)

		window := task.Window() + time.Hour
		px := int(window / time.Millisecond)

		args := []interface{}{lockKey, t.host, px, t.lastKey, task.Name(), lastLockValue}
		conn := t.connect()
		defer conn.Close()
		lockAquired, err := redis.Int(script.Do(conn, args...))
		if err != nil {
			return err
		}
		if lockAquired == 1 {
			return cb(task, timeIntendedToRun)
		}
		return nil
	}

}
