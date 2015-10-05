package schtick

import (
	"math"
	"testing"
	"time"
)

type runRecord struct {
	Actual   time.Time
	Intended time.Time
}

func (r *runRecord) secondsDifference() float64 {
	return math.Abs(r.Actual.Sub(r.Intended).Seconds())
}

func TestBasicExamples(t *testing.T) {
	var taskErr error
	schtick := New(func(task Task, err error) {
		taskErr = err
	})

	var allRecords []*runRecord
	all, err := schtick.AddTaskWithDefaults("all", "sec(*)", func(task Task, run time.Time) error {
		allRecords = append(allRecords, &runRecord{time.Now(), run})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var evenRecords []*runRecord
	even, err := schtick.AddTaskWithDefaults("even", "sec(*%2)", func(task Task, run time.Time) error {
		evenRecords = append(evenRecords, &runRecord{time.Now(), run})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// give them a chance to run
	time.Sleep(time.Second * 4)

	// look at the results
	all.StopSchedule()
	even.StopSchedule()

	if taskErr != nil {
		t.Fatal(taskErr)
	}

	allCount := len(allRecords)
	evenCount := len(evenRecords)

	if allCount < 3 || allCount > 5 {
		t.Fatalf("allCount was %d but should have been between 3 and 5", allCount)
	}

	if evenCount < 1 || evenCount > 3 {
		t.Fatalf("evenCount was %d but should have been between 1 and 3", evenCount)
	}

	// make sure all of the events are within 100 milliseconds of the intended time
	for _, r := range allRecords {
		if r.secondsDifference() > .1 {
			t.Errorf("Seconds difference: %s", r.secondsDifference())
		}
	}

	for _, r := range evenRecords {
		if r.secondsDifference() > .1 {
			t.Errorf("Seconds difference: %s", r.secondsDifference())
		}
	}
}
