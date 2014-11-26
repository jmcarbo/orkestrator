package main

import (
	"testing"
	"time"
)

func TestJobLongRunning(t *testing.T) {
	sche := initTest(t, "testscheduler2", "")
	sche.AddJob(&Job{ID: "strid", Command: "sleep 20 && echo hello world", LogOutput: false})
	sche.Start()
	time.Sleep(time.Second * 5)
	job, _ := sche.GetJob("strid")
	if job.runs[0].Status != "running" {
		t.Logf("%#v\n", job.runs)
		t.Fatal("Job status not running")
	}
	endTest(t)
}

func TestJobWithError(t *testing.T) {
	sche := initTest(t, "testscheduler2", "")
	sche.AddJob(&Job{ID: "errorjob", Command: "exit 24", LogOutput: false})
	sche.Start()
	time.Sleep(time.Second * 5)
	job, _ := sche.GetJob("errorjob")
	if job.runs[0].Status != "error" {
		t.Fatal("Job status not erroneous")
	}
	endTest(t)
}

func TestJobTimeoutError(t *testing.T) {
	sche := initTest(t, "testscheduler2", "")
	sche.AddJob(&Job{ID: "errorjobt", Command: "sleep 5", LogOutput: false, Timeout: 1})
	sche.Start()
	time.Sleep(time.Second * 5)
	job, _ := sche.GetJob("errorjobt")
	if job.runs[0].Status != "error" {
		t.Fatal("Job status not erroneous")
	}
	endTest(t)
}
