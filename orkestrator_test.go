package main

import (
  "fmt"
  "testing"
  "errors"
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
  client := Connect()
  _, err := client.KV().DeleteTree("jobs", nil)
  if err != nil {
    t.Fatal(err)
  }
  sche := NewScheduler("testschedule", client)
  err = sche.AddJob(&Job{ID:"blabla"})
  if err != nil {
    t.Fatal(err)
  }
}

func TestRunJob(t *testing.T) {
  client := Connect()
  _, err := client.KV().DeleteTree("jobs", nil)
  if err != nil {
    t.Fatal(err)
  }
  sche := NewScheduler("testschedule", client)
  err = sche.AddJob(&Job{ID:"blabla"})
  if err != nil {
    t.Fatal(err)
  }
  err = sche.RunJob("blabla")
  if err != nil {
    t.Fatal(err)
  }
}

func TestLockJob(t *testing.T) {
  client := Connect()
  _, err := client.KV().DeleteTree("jobs", nil)
  if err != nil {
    t.Fatal(err)
  }
  sche := NewScheduler("testschedule", client)
  err = sche.AddJob(&Job{ID:"blabla"})
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
  client := Connect()
  _, err := client.KV().DeleteTree("jobs", nil)
  if err != nil {
    t.Fatal(err)
  }
  sche := NewScheduler("testschedule", client)
  err = sche.AddJob(&Job{ID:"blabla"})
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
  client := Connect()
  kv := client.KV()
  _, err := kv.DeleteTree("jobs", nil)
  if err != nil {
    t.Fatal(err)
  }

  sche := NewScheduler("testschedule", client)

  for i:=0; i < 100; i++ {
    strid := fmt.Sprintf("blablabla%d", i)
    err = sche.AddJob(&Job{ID: strid})
    if err != nil {
      t.Fatal(err)
    }
  }


  keys, err := sche.ListJobs()

  if len(keys) != 100 {
    t.Fatalf("Wrong number of jobs. %d instead of 100", len(keys))
  }

}

func TestSchedulerAddExistantJob(t *testing.T) {
  client := Connect()
  _, err := client.KV().DeleteTree("jobs", nil)
  if err != nil {
    t.Fatal(err)
  }
  sche := NewScheduler("testschedule", client)
  err = sche.AddJob(&Job{ID:"blabla"})
  if err != nil {
    t.Fatal(err)
  }
  err = sche.AddJob(&Job{ID:"blabla"})
  if err == nil {
    t.Fatal(err)
  }
}

func TestSchedulerDeleteExistantJob(t *testing.T) {
  client := Connect()

  _, err := client.KV().DeleteTree("jobs", nil)
  if err != nil {
    t.Fatal(err)
  }

  se, _, _ := client.Session().List(nil)
  for _, s := range se {
    t.Logf("Destroying session %s", s.ID)
    client.Session().Destroy(s.ID, nil)
  }

  sche := NewScheduler("testschedule", client)
  err = sche.AddJob(&Job{ID:"blabla"})
  if err != nil {
    t.Fatal(err)
  }
  err = sche.DeleteJob("blabla")
  if err != nil {
    t.Fatal(err)
  }
}

func TestSchedulerDeleteNonExistantJob(t *testing.T) {
  client := Connect()

  _, err := client.KV().DeleteTree("jobs", nil)
  if err != nil {
    t.Fatal(err)
  }

  se, _, _ := client.Session().List(nil)
  for _, s := range se {
    t.Logf("Destroying session %s", s.ID)
    client.Session().Destroy(s.ID, nil)
  }

  sche := NewScheduler("testschedule", client)
  err = sche.DeleteJob("blabla")
  if err == nil {
    t.Fatal("No error deleting non existant job")
  }
}
func TestSchedulerTimeout(t *testing.T) {
  if testing.Short(){
    t.Skip("Skipping ...")
  }
  client := Connect()
  sche := NewScheduler("testschedule", client)
  c:=sche.Start()
  <-c
  t.Logf(sche.Status)
}


func TestSchedulerStop(t *testing.T) {
  if testing.Short(){
    t.Skip("Skipping ...")
  }
  client := Connect()
  sche := NewScheduler("testschedule", client)
  sche.Start()
  c:=time.After(time.Second*10)
  <-c
  sche.Stop()
  t.Logf(sche.Status)
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
