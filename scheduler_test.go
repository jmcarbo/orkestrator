package orkestrator

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
	if pending != 2 {
		t.Logf("total %d, pending %d, running %d, done %d, withErrors %d\n", total, pending, running, done, withErrors)
		t.Fatal("Pending jobs !=2 ")
	}
	sche1.Start()
	time.Sleep(time.Second * 1)
	total, pending, running, done, withErrors, _ = sche1.JobsCount()
	if running != 2 {
		t.Logf("total %d, pending %d, running %d, done %d, withErrors %d\n", total, pending, running, done, withErrors)
		t.Fatal("Running jobs !=2 ")
	}
	time.Sleep(time.Second * 5)
	total, pending, running, done, withErrors, _ = sche1.JobsCount()
	if done != 2 {
		t.Logf("total %d, pending %d, running %d, done %d, withErrors %d\n", total, pending, running, done, withErrors)
		t.Fatal("Done jobs !=2 ")
	}

}

func TestJobTwoSchedulersTwoJobs(t *testing.T) {
	sche1 := initTest(t, "testscheduler", "node1")
	sche2 := initTest(t, "testscheduler", "node2")
	sche1.AddJob(&Job{ID: "job1", Command: "sleep 2 && echo hello world 1", LogOutput: false})
	sche2.AddJob(&Job{ID: "job2", Command: "sleep 2 && echo hello world 2", LogOutput: false})
	sche1.Start()
	sche2.Start()
	sche1.WaitForDone(2)
	sche1.Stop()
	sche2.Stop()
	//time.Sleep(time.Second*5)
	job1, _ := sche2.GetJob("job1")
	job2, _ := sche1.GetJob("job2")
	if job1.runs[0].Status != "done" || job2.runs[0].Status != "done" {
		t.Logf("%#v\n", job1)
		t.Logf("%#v\n", job2)
		t.Fatal("Error running jobs")
	}
}

func TestJobTwoInstancesMaxInstancesPerNode1(t *testing.T) {
	sche1 := initTest(t, "testscheduler", "node1")
	sche2 := initTest(t, "testscheduler", "node2")
	sche1.AddJob(&Job{ID: "job1", Command: "sleep 2 && echo hello world 1", LogOutput: false, MaxInstances: 2, MaxInstancesPerNode: 1})
	sche1.Start()
	sche2.Start()
	sche1.WaitForDone(2)
	job1, _ := sche2.GetJob("job1")
	if !(job1.runs[0].Node != job1.runs[1].Node) {
		t.Logf("%#v\n", job1)
		t.Fatal("Two runs in same node")
	}
}

func TestTwoSchedulersTwoJobsNodeConstraint(t *testing.T) {
	sche1 := initTest(t, "testscheduler", "node1")
	sche2 := initTest(t, "testscheduler", "node2")
	sche1.AddJob(&Job{ID: "job1", Command: "sleep 2 && echo hello world 1", LogOutput: false, TargetNodes: []string{"node2"}})
	sche2.AddJob(&Job{ID: "job2", Command: "sleep 2 && echo hello world 2", LogOutput: false, TargetNodes: []string{"node1"}})
	sche1.Start()
	sche2.Start()
	sche1.WaitForDone(2)
	job1, _ := sche2.GetJob("job1")
	job2, _ := sche1.GetJob("job2")
	if job1.runs[0].Node != "node2" {
		t.Logf("%#v\n", job1)
		t.Fatal("Job1 should run in node2")
	}
	if job1.runs[0].Node != "node2" {
		t.Logf("%#v\n", job2)
		t.Fatal("Job2 should run in node1")
	}
}
