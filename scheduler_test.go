package orkestrator

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

/*
func TestJoblist(t *testing.T) {
  client := Connect()
  kv := client.KV()
  //session := client.Session()
  //agent := client.Agent()

  kv.DeleteTree("/test", nil)
  kv.Put(&consulapi.KVPair{Key:"/test/job", Value: &Job{ID: "vla", Command: "echo hello world", State: "Pending"}}, nil)
}
*/
func TestConnection(t *testing.T) {
	client := Connect()
	leader, err := client.Status().Leader()
	if err != nil {
		t.Fatal(err)
	}
	if leader == "" {
		t.Fatal(errors.New("No leader found"))
	}
	t.Log("Connected succesfully")
}

func TestSchedulerAddJob(t *testing.T) {
	sche := initTest(t, "testschedule", "")

	err := sche.AddJob(&Job{ID: "blabla"})
	if err != nil {
		t.Fatal(err)
	}
	kv := sche.Client.KV()
	_, _, err = kv.Get("jobs/testschedule/blabla", nil)
	if err != nil {
		t.Fatal(err)
	}
	//t.Logf("-----> %#v\n", kvp)
	//t.Logf("-----> %s\n", string(kvp.Value))
}

func TestRunJob(t *testing.T) {
	sche := initTest(t, "testschedule", "")
	err := sche.AddJob(&Job{ID: "blabla", Command: "docker ps"})
	if err != nil {
		t.Fatal(err)
	}
	err = sche.RunJob("blabla")
	if err != nil {
		t.Fatal(err)
	}

	job, _ := sche.GetJob("blabla")
	t.Logf("Job output: %s\n", job.Output)
}

func TestLockJob(t *testing.T) {
	sche := initTest(t, "testschedule", "")
	err := sche.AddJob(&Job{ID: "blabla"})
	if err != nil {
		t.Fatal(err)
	}
	sess, err := sche.LockJob("blabla")
	if err != nil {
		t.Fatal(err)
	}
	if sess == "" {
		t.Fatal("No session")
	}
	kvp, err := sche.GetJobKV("blabla")
	if err != nil {
		t.Fatal(err)
	}
	if kvp.Session == "" {
		t.Fatal("Job not Locked")
	}
}

func TestUnlockJob(t *testing.T) {
	sche := initTest(t, "testschedule", "")
	err := sche.AddJob(&Job{ID: "blabla"})
	if err != nil {
		t.Fatal(err)
	}
	sess, err := sche.LockJob("blabla")
	if err != nil {
		t.Fatal(err)
	}
	if sess == "" {
		t.Fatal("No session")
	}
	kvp, err := sche.GetJobKV("blabla")
	if err != nil {
		t.Fatal(err)
	}
	if kvp.Session == "" {
		t.Fatal("Job not Locked")
	}
	err = sche.UnlockJob("blabla")
	if err != nil {
		t.Fatal(err)
	}
	kvp, err = sche.GetJobKV("blabla")
	if err != nil {
		t.Fatal(err)
	}
	if kvp.Session != "" {
		t.Fatal("Job not unlocked")
	}
}

func TestSchedulerListJobs(t *testing.T) {
	sche := initTest(t, "testschedule", "")

	for i := 0; i < 100; i++ {
		strid := fmt.Sprintf("blablabla%d", i)
		err := sche.AddJob(&Job{ID: strid})
		if err != nil {
			t.Fatal(err)
		}
	}

	keys, _ := sche.ListJobs()

	if len(keys) != 100 {
		t.Fatalf("Wrong number of jobs. %d instead of 100", len(keys))
	}

}

func TestSchedulerAddExistantJob(t *testing.T) {
	sche := initTest(t, "testschedule", "")
	err := sche.AddJob(&Job{ID: "blabla"})
	if err != nil {
		t.Fatal(err)
	}
	err = sche.AddJob(&Job{ID: "blabla"})
	if err == nil {
		t.Fatal(err)
	}
}

func TestSchedulerDeleteExistantJob(t *testing.T) {
	sche := initTest(t, "testschedule", "")
	err := sche.AddJob(&Job{ID: "blabla"})
	if err != nil {
		t.Fatal(err)
	}
	err = sche.DeleteJob("blabla")
	if err != nil {
		t.Fatal(err)
	}
}

func TestSchedulerDeleteNonExistantJob(t *testing.T) {
	sche := initTest(t, "testschedule", "")
	err := sche.DeleteJob("blabla")
	if err == nil {
		t.Fatal("No error deleting non existant job")
	}
}

func TestSchedulerStart(t *testing.T) {
	/*
		if testing.Short() {
			t.Skip("Skipping ...")
		}
	*/
	maxjobs := 10
	sche := initTest(t, "testschedule2", "")
	for i := 0; i < maxjobs; i++ {
		strid := fmt.Sprintf("blablabla%d", i)
		err := sche.AddJob(&Job{ID: strid, Command: "echo hello world", LogOutput: false})
		if err != nil {
			t.Fatal(err)
		}
	}

	sche.Start()
	time.Sleep(time.Second * 5)
	sche.Stop()

	for i := 0; i < maxjobs; i++ {
		strid := fmt.Sprintf("blablabla%d", i)
		job, err := sche.GetJob(strid)
		if err != nil {
			t.Fatalf("Erro getting Job: %s\n", strid)
		} else {
			if job.Output != "hello world\n" {
				t.Fatalf("Job output incorrect: %s\n", job.Output)
			}
		}
	}

}

func TestSchedulerStop(t *testing.T) {
	/*
		if testing.Short() {
			t.Skip("Skipping ...")
		}
	*/
	sche := initTest(t, "testschedule", "")
	sche.Start()
	c := time.After(time.Second * 2)
	<-c
	sche.Stop()
	t.Logf(sche.GetStatus())
}

/*
func TestJoblist(t *testing.T) {
  client := Connect()
  kv := client.KV()
  //session := client.Session()
  //agent := client.Agent()

  kv.DeleteTree("/test", nil)
  kv.Put(&consulapi.KVPair{Key:"/test/job", Value: &Job{ID: "vla", Command: "echo hello world", State: "Pending"}}, nil)
}
*/
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
