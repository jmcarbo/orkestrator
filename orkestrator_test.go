package orkestrator

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

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
