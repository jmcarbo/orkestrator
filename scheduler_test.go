package main

import (
  "testing"
  "time"
)

func TestJobCount(t *testing.T) {
  sche1 := initTest(t, "testscheduler", "node1")
  sche1.AddJob(&Job{ID: "job1", Command: "sleep 2 && echo hello world 1", LogOutput: false})
  sche1.AddJob(&Job{ID: "job2", Command: "sleep 2 && echo hello world 2", LogOutput: false})
  total, pending, running, done, withErrors, err := sche1.JobsCount()

  if err != nil {
    t.Fatal(err)
  }
  t.Logf("total %d, pending %d, running %d, done %d, withErrors %d\n", total, pending, running, done, withErrors)
  sche1.Start()
  time.Sleep(time.Second*1)
  total, pending, running, done, withErrors, _ = sche1.JobsCount()
  t.Logf("total %d, pending %d, running %d, done %d, withErrors %d\n", total, pending, running, done, withErrors)
  time.Sleep(time.Second*5)
  total, pending, running, done, withErrors, _ = sche1.JobsCount()
  t.Logf("total %d, pending %d, running %d, done %d, withErrors %d\n", total, pending, running, done, withErrors)
}

func TestTwoSchedulersTwoJobs(t *testing.T) {
  sche1 := initTest(t, "testscheduler", "node1")
  sche2 := initTest(t, "testscheduler", "node2")
  sche1.AddJob(&Job{ID: "job1", Command: "sleep 2 && echo hello world 1", LogOutput: false})
  sche2.AddJob(&Job{ID: "job2", Command: "sleep 2 && echo hello world 2", LogOutput: false})
  sche1.Start()
  sche2.Start()
  _, _, _, done, _, _ := sche1.JobsCount()
  for done < 2   {
    _, _, _, done, _, _ = sche1.JobsCount()
  }
  sche1.Stop()
  sche2.Stop()
  //time.Sleep(time.Second*5)
  job1, _ :=sche2.GetJob("job1") 
  job2, _ :=sche1.GetJob("job2") 
  t.Logf("%#v\n", job1)
  t.Logf("%#v\n", job2)
}

func TestJobTwoInstancesMaxInstancesPerNode1(t *testing.T) {
  sche1 := initTest(t, "testscheduler", "node1")
  sche2 := initTest(t, "testscheduler", "node2")
  sche1.AddJob(&Job{ID: "job1", Command: "sleep 2 && echo hello world 1", LogOutput: false, MaxInstances: 2, MaxInstancesPerNode: 1})
  sche1.Start()
  sche2.Start()
  time.Sleep(time.Second*5)
  job1, _ :=sche2.GetJob("job1") 
  t.Logf("%#v\n", job1)
}

func TestTwoSchedulersTwoJobsNodeConstraint(t *testing.T) {
  sche1 := initTest(t, "testscheduler", "node1")
  sche2 := initTest(t, "testscheduler", "node2")
  sche1.AddJob(&Job{ID: "job1", Command: "sleep 2 && echo hello world 1", LogOutput: false, TargetNodes: []string{"node2"}})
  sche2.AddJob(&Job{ID: "job2", Command: "sleep 2 && echo hello world 2", LogOutput: false, TargetNodes: []string{"node1"}})
  sche1.Start()
  sche2.Start()
  time.Sleep(time.Second*5)
  job1, _ :=sche2.GetJob("job1") 
  job2, _ :=sche1.GetJob("job2") 
  t.Logf("%#v\n", job1)
  t.Logf("%#v\n", job2)
}
