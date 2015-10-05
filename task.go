package schtick

import (
	"errors"
	"github.com/schyntax/go-schyntax"
	"sync"
	"sync/atomic"
	"time"
)

type Task interface {
	Name() string
	Schedule() schyntax.Schedule
	Callback() func(task Task, timeIntendedToRun time.Time) error
	IsScheduleRunning() bool
	IsAttached() bool
	Window() time.Duration
	SetWindow(window time.Duration)
	NextEvent() time.Time
	PrevEvent() time.Time
	StartSchedule() error
	StartFromLastKnownEvent(lastKnownEvent time.Time) error
	StopSchedule()
	UpdateScheduleString(schedule string) error
	UpdateSchedule(schedule schyntax.Schedule) error
}

type ScheduleCrashError struct {
	message    string
	InnerError error
}

func (e *ScheduleCrashError) Error() string {
	return e.message
}

var _ Task = &taskImpl{}

type taskImpl struct {
	scheduleLock      sync.Mutex
	runId             int
	execLocked        int32
	schtick           *schtickImpl
	name              string
	schedule          schyntax.Schedule
	callback          func(task Task, timeIntendedToRun time.Time) error
	isScheduleRunning bool
	isAttached        bool
	window            time.Duration
	nextEvent         time.Time
	prevEvent         time.Time
}

func (t *taskImpl) Name() string {
	return t.name
}

func (t *taskImpl) Schedule() schyntax.Schedule {
	return t.schedule
}

func (t *taskImpl) Callback() func(task Task, timeIntendedToRun time.Time) error {
	return t.callback
}

func (t *taskImpl) IsScheduleRunning() bool {
	return t.isScheduleRunning
}

func (t *taskImpl) IsAttached() bool {
	return t.isAttached
}

func (t *taskImpl) Window() time.Duration {
	return t.window
}

func (t *taskImpl) SetWindow(window time.Duration) {
	t.window = window
}

func (t *taskImpl) NextEvent() time.Time {
	return t.nextEvent
}

func (t *taskImpl) PrevEvent() time.Time {
	return t.prevEvent
}

func (t *taskImpl) StartSchedule() error {
	var tm time.Time
	return t.StartFromLastKnownEvent(tm)
}

func (t *taskImpl) StartFromLastKnownEvent(lastKnownEvent time.Time) error {
	t.scheduleLock.Lock()
	defer t.scheduleLock.Unlock()

	if !t.isAttached {
		return errors.New("Cannot start task which is not attached to a Schtick instance.")
	}

	if t.isScheduleRunning {
		return nil
	}

	var defaultTime time.Time
	var firstEvent time.Time
	var firstEventSet = false

	if t.window > 0 && lastKnownEvent.Equal(defaultTime) {
		// check if we actually want to run the first event right away
		prev, err := t.schedule.Previous()
		if err != nil {
			return err
		}

		lastKnownEvent = lastKnownEvent.Add(time.Second) // add a second for good measure
		if prev.After(lastKnownEvent) && prev.After(time.Now().Add(-t.window)) {
			firstEvent = prev
			firstEventSet = true
		}
	}

	if !firstEventSet {
		var err error
		firstEvent, err = t.schedule.Next()
		if err != nil {
			return err
		}
	}

	for !firstEvent.After(t.prevEvent) {
		// we don't want to run the same event twice
		var err error
		firstEvent, err = t.schedule.NextAfter(firstEvent)
		if err != nil {
			return err
		}
	}

	t.nextEvent = firstEvent
	t.isScheduleRunning = true
	t.queueNextEvent()

	return nil
}

func (t *taskImpl) StopSchedule() {
	t.scheduleLock.Lock()
	defer t.scheduleLock.Unlock()

	if t.isScheduleRunning {
		t.runId++
		t.isScheduleRunning = false
	}
}

func (t *taskImpl) UpdateScheduleString(schedule string) error {
	if schedule == t.schedule.OriginalText() {
		return nil
	}

	sch, err := schyntax.New(schedule)
	if err != nil {
		return err
	}

	return t.UpdateSchedule(sch)
}

func (t *taskImpl) UpdateSchedule(schedule schyntax.Schedule) error {
	if schedule == nil {
		return errors.New("schedule argument cannot be nil")
	}

	t.scheduleLock.Lock()
	defer t.scheduleLock.Unlock()

	wasRunning := t.isScheduleRunning
	if wasRunning {
		t.StopSchedule()
	}

	t.schedule = schedule

	if wasRunning {
		return t.StartSchedule()
	}

	return nil
}

func (t *taskImpl) runPendingEvent(ev *pendingEvent) {
	t.executeEventInternal(ev)

	t.scheduleLock.Lock()
	defer t.scheduleLock.Unlock()

	err := t.setNextEvent(ev)
	if err != nil {
		t.runId++
		t.isScheduleRunning = false
		t.raiseError(&ScheduleCrashError{"Schtick Schedule has been terminated because the next valid time could not be found.", err})
	}
}

func (t *taskImpl) executeEventInternal(ev *pendingEvent) {
	execLockTaken := t.tryTakeExecLock(ev)
	if execLockTaken {
		defer func() { t.execLocked = 0 }()
	}

	if execLockTaken {
		err := t.callback(t, ev.ScheduledTime)
		if err != nil {
			t.raiseError(err)
		}
	}
}

func (t *taskImpl) tryTakeExecLock(ev *pendingEvent) bool {
	t.scheduleLock.Lock()
	defer t.scheduleLock.Unlock()

	if t.runId != ev.RunId {
		return false
	}

	execLockTaken := atomic.CompareAndSwapInt32(&t.execLocked, 0, 1)
	if execLockTaken {
		t.prevEvent = ev.ScheduledTime // set this here while we're still in the schedule lock
	}

	return execLockTaken
}

func (t *taskImpl) setNextEvent(ev *pendingEvent) error {
	// figure out the next time to run the schedule
	if ev.RunId != t.runId {
		return nil
	}

	next, err := t.schedule.Next()
	if err != nil {
		return err
	}

	if !next.After(ev.ScheduledTime) {
		next, err = t.schedule.NextAfter(ev.ScheduledTime)
		if err != nil {
			return err
		}
	}

	t.nextEvent = next
	t.queueNextEvent()

	return nil
}

func (t *taskImpl) queueNextEvent() {
	t.schtick.addPendingEvent(&pendingEvent{t.nextEvent, t, t.runId})
}

func (t *taskImpl) raiseError(err error) {
	go t.schtick.errorHandler(t, err)
}
