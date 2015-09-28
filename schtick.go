package schtick

import (
	"errors"
	"github.com/satori/go.uuid"
	"github.com/schyntax/go-schyntax"
	"sync"
	"time"
)

type Options struct {
	Name           string
	ScheduleString string
	Schedule       schyntax.Schedule
	Window         time.Duration
	LastKnownEvent time.Time
	DisableAutoRun bool
}

type Schtick interface {
	AddTask(options Options) Task
	GetTaskByName(name string) Task
	RemoveTask(name string) bool
	IsShuttingDown() bool
	Shutdown()
}

var _ Schtick = &schtickImpl{}

type schtickImpl struct {
	tasksLock      sync.RWMutex
	tasks          map[string]*taskImpl
	heapLock       sync.RWMutex
	heap           pendingEventHeap
	isShuttingDown bool
}

func New() Schtick {
	// todo
}

func (s *schtickImpl) AddTask(options Options) (Task, error) {
	hasStr := options.ScheduleString != ""
	hasObj := options.Schedule != nil

	if hasStr == hasObj {
		return nil, errors.New("Schtick.AddTask: Must provide either ScheduleString or Schedule, but not both.")
	}

	sch := options.Schedule
	if hasStr {
		var err error
		sch, err = schyntax.New(options.ScheduleString)
		if err != nil {
			return nil, err
		}
	}

	name := options.Name
	if name == "" {
		name = uuid.NewV4().String()
	}

	s.tasksLock.Lock()
	defer s.tasksLock.Unlock()

	if s.tasks[name] != nil {
		return nil, errors.New(`A scheduled task named "` + name + `" already exists.`)
	}

	task := taskImpl{}
	task.schtick = s
	task.name = name
	task.schedule = sch
	task.window = options.Window
	task.isAttached = true

	// add

	// auto run

	return task
}

func (s *schtickImpl) GetTaskByName(name string) Task {
	s.tasksLock.RLock()
	defer s.tasksLock.RUnlock()

	return s.tasks[name]
}

func (s *schtickImpl) RemoveTask(name string) bool {
	// todo
}

func (s *schtickImpl) IsShuttingDown() bool {
	return s.isShuttingDown
}

func (s *schtickImpl) Shutdown() {
	// todo
}

func (s *schtickImpl) poll() {
	// todo
}
