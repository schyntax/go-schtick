package schtick

import (
	"github.com/schyntax/go-schyntax"
	"time"
)

type Options struct {
	Name string
	ScheduleString string
	Schedule schyntax.Schedule
	Window time.Duration
	LastKnownEvent time.Time
	DisableAutoRun bool
}

type Task interface {
	Name() string
	Schedule() schyntax.Schedule
	IsScheduleRunning() bool
	IsAttached() bool
	Window() time.Duration
	SetWindow(window time.Duration)
	NextEvent() time.Time
	PrevEvent() time.Time
	StartSchedule()
	StopSchedule()
	UpdateScheduleString(schedule string)
	UpdateSchedule(schedule schyntax.Schedule)
}

type taskImpl struct {
	//
}

func (t *taskImpl) runPendingEvent(ev *pendingEvent) {
	//
}
