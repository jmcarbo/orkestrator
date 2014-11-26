package main

import (
  "testing"
  "time"
)


func TestLongRunningJob(t *testing.T) {
  sche := initTest(t, "testscheduler2", "")
  sche.AddJob(&Job{ID: "strid", Command: "sleep 20 && echo hello world", LogOutput: false})
  sche.Start()
  time.Sleep(time.Second*5)
  job, _ :=sche.GetJob("strid") 
  t.Logf("%#v\n", job.runs)
}
