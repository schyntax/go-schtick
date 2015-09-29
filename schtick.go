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
	Callback       func(task Task, timeIntendedToRun time.Time) error
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
	errorHandler   func(task Task, err error)
}

func New(errorHandler func(task Task, err error)) Schtick {
	s := &schtickImpl{}
	s.errorHandler = errorHandler

	go s.poll()

	return s
}

func (s *schtickImpl) AddTask(options Options) (Task, error) {
	if options.Callback == nil {
		return nil, errors.New("Schtick.AddTask: Must provide a callback function.")
	}

	// setup or validate the Schedule object
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

	// make sure the task has a name
	name := options.Name
	if name == "" {
		name = uuid.NewV4().String()
	}

	// lock tasks
	s.tasksLock.Lock()
	defer s.tasksLock.Unlock()

	if s.tasks[name] != nil {
		return nil, errors.New(`A scheduled task named "` + name + `" already exists.`)
	}

	// create the task
	task := taskImpl{}
	task.schtick = s
	task.name = name
	task.schedule = sch
	task.callback = options.Callback
	task.window = options.Window
	task.isAttached = true

	s.tasks[name] = task

	if !options.DisableAutoRun {
		task.StartFromLastKnownEvent(options.LastKnownEvent)
	}

	return task
}

func (s *schtickImpl) GetTaskByName(name string) Task {
	s.tasksLock.RLock()
	defer s.tasksLock.RUnlock()

	return s.tasks[name]
}

func (s *schtickImpl) GetAllTasks() []Task {
	s.tasksLock.RLock()
	defer s.tasksLock.RUnlock()

	var tasks []Task
	for _, t := range s.tasks {
		tasks = append(tasks, t)
	}

	return tasks
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

func (s *schtickImpl) addPendingEvent(ev *pendingEvent) {
	if s.IsShuttingDown() { // don't care about adding anything if we're shutting down
		return
	}

	s.heapLock.Lock()
	defer s.heapLock.Unlock()

	s.heap.Push(ev)
}

func (s *schtickImpl) poll() {
	// figure out the initial delay
	now := time.Now().UTC()
	intendedTime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), 0, time.UTC)
	if now.Nanosecond() > 0 {
		intendedTime = intendedTime.Add(time.Second)
		time.Sleep(intendedTime.Sub(now))
	}

	for {
		if s.IsShuttingDown() {
			return
		}

		s.popAndRunEvents(intendedTime)

		// figure out the next second to poll on
		now = time.Now().UTC()
		for {
			intendedTime = intendedTime.Add(time.Second)
			if !now.After(intendedTime) {
				break
			}
		}

		waitDur := intendedTime.Sub(now)
		if waitDur > 0 {
			time.Sleep(waitDur)
		}
	}
}

func (s *schtickImpl) popAndRunEvents(intendedTime *time.Time) {
	s.heapLock.Lock()
	defer s.heapLock.Unlock()

	for s.heap.Count() > 0 && !s.heap.Peek().ScheduledTime.After(intendedTime) {
		s.heap.Pop().Run() // spins up a go routine for the task
	}
}
