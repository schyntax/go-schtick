package schtick

import (
	"github.com/schyntax/go-schyntax"
	"sync"
	"time"
)

type Task interface {
	Name() string
	Schedule() *schyntax.Schedule
	IsScheduleRunning() bool
	IsAttached() bool
	Window() time.Duration
	SetWindow(window time.Duration)
	NextEvent() time.Time
	PrevEvent() time.Time
	StartSchedule()
	StopSchedule()
	UpdateScheduleString(schedule string) error
	UpdateSchedule(schedule *schyntax.Schedule)
}

var _ Task = &taskImpl{}

type taskImpl struct {
	scheduleLock      sync.Mutex
	runId             int
	execLocked        int32
	schtick           *schtickImpl
	name              string
	schedule          schyntax.Schedule
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

func (t *taskImpl) StartSchedule() {
	// todo
}

func (t *taskImpl) StopSchedule() {
	// todo
}

func (t *taskImpl) UpdateScheduleString(schedule string) error {
	if schedule == t.schedule.OriginalText() {
		return nil
	}

	sch, err := schyntax.New(schedule)
	if err != nil {
		return err
	}

	t.UpdateSchedule(sch)
	return nil
}

func (t *taskImpl) UpdateSchedule(schedule schyntax.Schedule) {
	// todo
}

func (t *taskImpl) runPendingEvent(ev *pendingEvent) {
	// todo
}
