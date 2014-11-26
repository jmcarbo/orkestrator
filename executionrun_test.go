package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func initTest(t *testing.T, schedulerName string, nodeName string) *Scheduler {
	client := Connect()

	jKey := fmt.Sprintf("jobs/%s/", schedulerName)
	_, err := client.KV().DeleteTree(jKey, nil)
	if err != nil {
		t.Fatal(err)
	}

	se, _, _ := client.Session().List(nil)
	for _, s := range se {
		//t.Logf("Destroying session %s", s.ID)
		client.Session().Destroy(s.ID, nil)
	}

	sche := NewScheduler(schedulerName, client, nodeName)
	return sche
}

func endTest(t *testing.T) {
	client := Connect()
	jKey := fmt.Sprintf("jobs/")
	_, err := client.KV().DeleteTree(jKey, nil)
	if err != nil {
		t.Fatal(err)
	}

	se, _, _ := client.Session().List(nil)
	for _, s := range se {
		//t.Logf("Destroying session %s", s.ID)
		client.Session().Destroy(s.ID, nil)
	}
	/*
		checks, err := client.Agent().Checks()
		for check, _ := range checks {
			if err := client.Agent().CheckDeregister(check); err != nil {
				t.Fatal(err)
			}
		}
	*/
}

func TestSaveRun(t *testing.T) {
	sche := initTest(t, "tests", "")

	var j Job
	var er []ExecutionRun
	j.ID = "aaa"
	er = append(er, ExecutionRun{ID: "aa", Node: "node1"})
	er = append(er, ExecutionRun{ID: "ab", Node: "node1"})
	er = append(er, ExecutionRun{ID: "ac", Node: "node2"})
	j.runs = er

	err := sche.SaveRuns(&j)
	if err != nil {
		t.Fatal(err)
	}

	kv := sche.Client.KV()
	for _, r := range er {
		key := fmt.Sprintf("jobs/tests/aaa/runs/%s/%s", r.Node, r.ID)
		kvp, _, err := kv.Get(key, nil)
		if err != nil {
			t.Fatal(err)
		}
		if kvp == nil {
			t.Fatal("no run key")
		}

		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal("Error encoding json")
		}

		if string(b) != string(kvp.Value) {
			t.Fatal("Json values don't match")
		}
	}
	endTest(t)
}

func TestLoadRun(t *testing.T) {
	sche := initTest(t, "tests", "")

	var j Job
	var er []ExecutionRun
	j.ID = "aaa"
	er = append(er, ExecutionRun{ID: "aa", Node: "node1"})
	er = append(er, ExecutionRun{ID: "ab", Node: "node1"})
	er = append(er, ExecutionRun{ID: "ac", Node: "node2"})
	j.runs = er

	err := sche.SaveRuns(&j)
	if err != nil {
		t.Fatal(err)
	}

	var j2 Job
	j2.ID = "aaa"
	err = sche.LoadRuns(&j2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(j, j2) {
		t.Fatal("Jobs don't match")
	}
	endTest(t)
}
