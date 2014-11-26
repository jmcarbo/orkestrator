package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/armon/consul-api"
	"github.com/codegangsta/cli"
	"github.com/jmcarbo/go-sh"
	"github.com/nu7hatch/gouuid"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	execution_timeout = time.Duration(60)
)

func OutputAll(s *sh.Session) (out []byte, oerr []byte, err error) {
	oldout := s.Stdout
	olderr := s.Stderr
	defer func() {
		s.Stdout = oldout
		s.Stderr = olderr
	}()
	stdout := bytes.NewBuffer(nil)
	stderr := bytes.NewBuffer(nil)
	s.Stdout = stdout
	s.Stderr = stderr
	err = s.Run()
	out = stdout.Bytes()
	oerr = stderr.Bytes()
	return
}

type Scheduler struct {
	ID       string
	Name     string
	Policy   string
	Client   *consulapi.Client
	Mutex    sync.Mutex
	nodeName string
}

func NewScheduler(name string, client *consulapi.Client, nodeName string) *Scheduler {
	var m sync.Mutex
	var err error

	kv := client.KV()
	agent := client.Agent()
	if nodeName == "" {
		nodeName, err = agent.NodeName()
		if err != nil {
			return nil
		}
	}

	jKey := fmt.Sprintf("schedulers/%s/%s", nodeName, name)
	jkv, _, _ := kv.Get(jKey, nil)
	if jkv == nil {
		jkv2 := &consulapi.KVPair{Key: jKey, Value: []byte("stopped")}
		_, err := kv.Put(jkv2, nil)
		if err != nil {
			return nil
		}
	}
	return &Scheduler{ID: name, Name: name, Policy: "", Client: client, Mutex: m, nodeName: nodeName}
}

func (s *Scheduler) WaitForDone(target int) error {
	_, _, _, done, _, err := s.JobsCount()
	if err != nil {
		return err
	}
	for done < target {
		_, _, _, done, _, err = s.JobsCount()

		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) SetStatus(status string) error {
	kv := s.Client.KV()
	jKey := fmt.Sprintf("schedulers/%s/%s", s.nodeName, s.ID)
	jkv := &consulapi.KVPair{Key: jKey, Value: []byte(status)}
	_, err := kv.Put(jkv, nil)
	if err != nil {
		return err
	}
	return nil
}

func (s *Scheduler) GetStatus() string {
	kv := s.Client.KV()
	jKey := fmt.Sprintf("schedulers/%s/%s", s.nodeName, s.ID)
	jkv, _, err := kv.Get(jKey, nil)
	if err != nil {
		return ""
	}
	return string(jkv.Value)
}

func (s *Scheduler) JobsCount() (total, pending, running, done, withErrors int, isError error) {
	kvp, err := s.ListJobs()
	if err != nil {
		isError = err
		return
	}
	for _, p := range kvp {
		parts := strings.Split(p.Key, "/")
		job, _ := s.GetJob(parts[2])
		total++
		if len(job.runs) == 0 {
			pending++
		}
		for _, r := range job.runs {
			switch r.Status {
			case "running":
				running++
			case "done":
				done++
			case "error":
				withErrors++
			}
		}
	}
	return
}

func (s *Scheduler) AddJob(job *Job) error {
	jKey := fmt.Sprintf("jobs/%s/%s", s.ID, job.ID)
	kv := s.Client.KV()
	cjkv, _, err := kv.Get(jKey, nil)
	if cjkv != nil {
		return errors.New("JobExists")
	}

	if job.MaxInstancesPerNode == 0 {
		job.MaxInstancesPerNode = -1
	}
	if job.MaxInstances == 0 {
		job.MaxInstances = 1
	}
	b, err := json.Marshal(job)
	if err != nil {
		return err
	}

	jkv := &consulapi.KVPair{Key: jKey, Value: b}
	_, err = kv.Put(jkv, nil)
	if err != nil {
		//log.Printf("Error adding %#v\n", jkv)
		return err
	}

	jKey = fmt.Sprintf("jobs/%s/%s/runs", s.ID, job.ID)
	jkv = &consulapi.KVPair{Key: jKey}
	_, err = kv.Put(jkv, nil)
	if err != nil {
		//log.Printf("Error adding %#v\n", jkv)
		return err
	}

	return nil
}

func (s *Scheduler) SaveRun(job *Job, er *ExecutionRun) error {
	kv := s.Client.KV()
	b, err := json.Marshal(er)
	if err != nil {
		return err
	}
	jKey := fmt.Sprintf("jobs/%s/%s/runs/%s/%s", s.ID, job.ID, er.Node, er.ID)
	//log.Println(jKey)
	jkv := &consulapi.KVPair{Key: jKey, Value: b}
	_, err = kv.Put(jkv, nil)
	if err != nil {
		return err
	}
	return nil
}

func (s *Scheduler) SaveRuns(job *Job) error {
	kv := s.Client.KV()
	for _, r := range job.runs {
		b, err := json.Marshal(r)
		if err != nil {
			return err
		}
		jKey := fmt.Sprintf("jobs/%s/%s/runs/%s/%s", s.ID, job.ID, r.Node, r.ID)
		//log.Println(jKey)
		jkv := &consulapi.KVPair{Key: jKey, Value: b}
		_, err = kv.Put(jkv, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) LoadRuns(job *Job) error {
	kv := s.Client.KV()
	runs := fmt.Sprintf("jobs/%s/%s/runs/", s.ID, job.ID)
	keys, _, err := kv.List(runs, nil)
	if err != nil {
		return err
	}

	for _, pair := range keys {
		var e ExecutionRun
		dec := json.NewDecoder(strings.NewReader(string(pair.Value)))
		if err := dec.Decode(&e); err == io.EOF {
			return err
		} else if err != nil {
			return err
		}
		e.Session = pair.Session
		job.runs = append(job.runs, e)
	}
	return nil
}

func (s *Scheduler) SaveJob(job *Job) error {
	jKey := fmt.Sprintf("jobs/%s/%s", s.ID, job.ID)
	kv := s.Client.KV()

	b, err := json.Marshal(job)
	if err != nil {
		return err
	}

	jkv := &consulapi.KVPair{Key: jKey, Value: b, Session: job.Session}
	_, err = kv.Put(jkv, nil)
	if err != nil {
		return err
	}

	//s.SaveRuns(job)
	return nil
}

func (s *Scheduler) GetJobKV(jobID string) (*consulapi.KVPair, error) {
	jKey := fmt.Sprintf("jobs/%s/%s", s.ID, jobID)
	//log.Printf("Getting JOBKV %s\n", jKey)
	kv := s.Client.KV()
	jkv, _, err := kv.Get(jKey, nil)
	if err != nil {
		return nil, err
	}
	//log.Printf("%#v\n", jkv)
	return jkv, nil
}

func (s *Scheduler) AgentName() string {
	return s.nodeName
}

func (s *Scheduler) updateCheck(check string) {
	agent := s.Client.Agent()
	for {
		time.Sleep(time.Second * 8)
		err := agent.PassTTL(check, "")
		if err != nil {
			break
		}
	}
}

func (s *Scheduler) LockExecutionRun(job *Job, er *ExecutionRun) (string, error) {
	if job.CheckCommand == "" {
		return "", errors.New("NoCheckCommand")
	}
	if job.CheckInterval == "" {
		job.CheckInterval = "30s"
	}

	jKey := fmt.Sprintf("jobs/%s/%s/runs/%s/%s", s.ID, job.ID, er.Node, er.ID)
	kv := s.Client.KV()
	jkv, _, err := kv.Get(jKey, nil)
	if err != nil {
		return "", err
	}
	if jkv == nil {
		return "", errors.New("NonExistantRun")
	}

	if jkv.Session != "" {
		return "", errors.New(fmt.Sprintf("Session %s locks run", jkv.Session))
	}

	session := s.Client.Session()
	uid, _ := uuid.NewV4()
	agent := s.Client.Agent()

	err = agent.CheckRegister(&consulapi.AgentCheckRegistration{uid.String(), uid.String(), "",
		consulapi.AgentServiceCheck{Interval: job.CheckInterval, Script: job.CheckCommand}})
	if err != nil {
		return "", err
	}
	log.Printf("Execution check register %s", uid.String())
	//TODO convert job.CheckInterval to integer
	dur, _ := time.ParseDuration(job.CheckInterval)
	time.Sleep(time.Second * dur)
	ses, _, err := session.Create(&consulapi.SessionEntry{Checks: []string{uid.String()}}, nil)
	//ses, _, err:= session.CreateNoChecks(nil,nil)
	if err != nil {
		return "", err
	}

	jkv.Session = ses
	res, _, err := kv.Acquire(jkv, nil)
	if err != nil {
		return "", err
	}

	if res == false {
		return "", errors.New("Can't lock run")
	}

	return ses, nil
}

func (s *Scheduler) LockJob(jobID string) (string, error) {
	jKey := fmt.Sprintf("jobs/%s/%s", s.ID, jobID)
	kv := s.Client.KV()
	jkv, _, err := kv.Get(jKey, nil)
	if err != nil {
		return "", err
	}
	if jkv == nil {
		return "", errors.New("NonExistantJob")
	}

	if jkv.Session != "" {
		return "", errors.New(fmt.Sprintf("Session %s locks job", jkv.Session))
	}

	session := s.Client.Session()
	uid, _ := uuid.NewV4()
	agent := s.Client.Agent()

	err = agent.CheckRegister(&consulapi.AgentCheckRegistration{uid.String(), uid.String(), "", consulapi.AgentServiceCheck{TTL: "10s"}})
	if err != nil {
		return "", err
	}
	err = agent.PassTTL(uid.String(), "")
	if err != nil {
		agent.CheckDeregister(uid.String())
		return "", err
	}

	log.Printf("Job check register %s", uid.String())
	go s.updateCheck(uid.String())

	ses, _, err := session.Create(&consulapi.SessionEntry{Checks: []string{uid.String()}}, nil)
	//ses, _, err:= session.CreateNoChecks(nil,nil)
	if err != nil {
		agent.CheckDeregister(uid.String())
		return "", err
	}

	jkv.Session = ses

	res, _, err := kv.Acquire(jkv, nil)
	if err != nil {
		agent.CheckDeregister(uid.String())
		return "", err
	}

	if res == false {
		agent.CheckDeregister(uid.String())
		return "", errors.New("Can't lock job")
	}

	return ses, nil
}

func (s *Scheduler) UnlockJob(jobID string) error {
	jKey := fmt.Sprintf("jobs/%s/%s", s.ID, jobID)
	kv := s.Client.KV()
	jkv, _, err := kv.Get(jKey, nil)
	if err != nil {
		return err
	}
	if jkv == nil {
		return errors.New("NonExistantJob")
	}

	if jkv.Session == "" {
		return errors.New("JobNotLocked")
	}

	sess := jkv.Session
	res, _, err := kv.Release(jkv, nil)
	if err != nil {
		return err
	}

	if res == false {
		return errors.New("Can't unlock job")
	}

	session := s.Client.Session()
	sesinfo, _, err := session.Info(sess, nil)
	if err != nil {
		return err
	}
	for _, sic := range sesinfo.Checks {
		agent := s.Client.Agent()
		log.Printf("Check deregister %s\n", sic)
		agent.CheckDeregister(sic)
	}
	_, err = session.Destroy(sess, nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scheduler) GetJob(jobid string) (Job, error) {
	//jKey := fmt.Sprintf("jobs/%s/%s", s.ID, jobid)
	var j Job
	jkv, err := s.GetJobKV(jobid)
	if err != nil {
		return j, err
	}
	if jkv == nil {
		//log.Printf(">>>>>>>>> %s >>>>>>>>>>>>>>>>>>>>>>>>>> %s\n", jobid, jKey)
		return j, errors.New("Invalid jobid")
	}

	dec := json.NewDecoder(strings.NewReader(string(jkv.Value)))
	//for {
	if err := dec.Decode(&j); err == io.EOF {
		//break
		return j, err
	} else if err != nil {
		return j, err
	}
	//}

	s.LoadRuns(&j)
	return j, nil
}

func exitedSuccesfully(cmd *exec.Cmd) bool {
	if cmd.ProcessState != nil {
		return cmd.ProcessState.Success()
	}
	return false
}
func (s *Scheduler) ExecutionRun(job *Job) error {
	var er ExecutionRun
	uu, _ := uuid.NewV4()
	er.ID = uu.String()
	//if job.LogOutput {
	log.Printf("Executing **** %s\n", job.ID)
	//}
	er.StartTime = time.Now().UnixNano()
	job.StartTime = time.Now().UnixNano()
	er.StartTimeStr = fmt.Sprintln(time.Now())
	job.StartTimeStr = fmt.Sprintln(time.Now())
	er.Node = s.AgentName()
	er.Status = "running"
	s.SaveRun(job, &er)

	job.ExecutionNode = s.AgentName()
	if job.NoWait == true {
		sss := sh.Command("/bin/bash", "-c", string(job.Command))
		sss.Stdout = ioutil.Discard
		sss.Stderr = ioutil.Discard
		sss.Start()
	} else {
		lapsus := execution_timeout
		if job.Timeout > 0 {
			lapsus = job.Timeout
		}
		sss := sh.Command("/bin/bash", "-c", string(job.Command)).SetTimeout(lapsus * time.Second)
		out, stderr, err := OutputAll(sss)
		if string(out) != "" && job.LogOutput {
			log.Printf("Output job %s **** %s\n", job.ID, string(out))
		}
		if string(stderr) != "" && job.LogOutput {
			log.Printf("Error job %s **** %s\n", job.ID, string(stderr))
		}
		if err != nil {
			log.Printf("Exit error %v", err)
			job.ExitErrors = fmt.Sprintf("%v", err)
		}
		er.Output = string(out)
		job.Output = string(out)
		er.OutputErrors = string(stderr)
		job.OutputErrors = string(stderr)

		//log.Printf("************** %#v\n", sss.Commands()[0].ProcessState)
		if exitedSuccesfully(sss.Commands()[0]) {
			job.Status = "done"
			er.Status = "done"
		} else {
			job.Status = "error"
			er.Status = "error"
		}
	}
	job.EndTime = time.Now().UnixNano()
	er.EndTime = time.Now().UnixNano()
	job.EndTimeStr = fmt.Sprintln(time.Now())
	er.EndTimeStr = fmt.Sprintln(time.Now())
	if job.CheckCommand != "" && job.Status == "done" {
		job.Status = "running"
		er.Status = "running"
		s.SaveRun(job, &er)
		sess, err := s.LockExecutionRun(job, &er)
		if err != nil {
			log.Println(err)
		}
		log.Printf("Session ---------------- %s", sess)
	} else {
		s.SaveRun(job, &er)
	}
	return nil
}

func (s *Scheduler) ShouldRun(job *Job) bool {
	var liveExecutions, finishedExecutions, errorExecutions, totalExecutions,
		nodeExecutions, nodeErrorExecutions, nodeLiveExecutions, nodeFinishedExecutions int

	for _, r := range job.runs {
		totalExecutions++
		if r.Node == s.nodeName {
			nodeExecutions++
			if r.Status == "running" {
				nodeLiveExecutions++
			}
			if r.Status == "done" {
				nodeFinishedExecutions++
			}
			if r.Status == "error" {
				nodeErrorExecutions++
			}
		}

		switch r.Status {
		case "running":
			if r.Session == "" {
				errorExecutions++
				r.Status = "error"
				s.SaveRun(job, &r)
			} else {
				liveExecutions++
			}
		case "done":
			finishedExecutions++
		case "error":
			errorExecutions++
		}
	}

	if job.AllNodes && nodeExecutions == 0 {
		return true
	}

	if totalExecutions == 0 || totalExecutions < job.MaxInstances {
		if len(job.TargetNodes) > 0 {
			for _, tn := range job.TargetNodes {
				if tn == s.nodeName && (nodeExecutions == 0 || nodeExecutions < job.MaxInstancesPerNode || job.MaxInstancesPerNode == -1) {
					return true
				}
			}
		} else {
			if nodeExecutions < job.MaxInstancesPerNode || job.MaxInstancesPerNode == -1 {
				return true
			} else {
				return false
			}
		}
	}

	return false
}

func (s *Scheduler) RunJob(jobID string) error {
	job, err := s.GetJob(jobID)
	sess, err := s.LockJob(jobID)
	job.Session = sess
	defer s.UnlockJob(jobID)
	if err != nil {
		return err
	}

	log.Println("1Calling should run")
	if !s.ShouldRun(&job) {
		return errors.New("Job should not run in this node")
	}
	log.Println("2Calling execution run")
	s.ExecutionRun(&job)

	log.Println("3Calling save run")
	s.SaveJob(&job)

	return nil
}

func (s *Scheduler) DeleteJob(jobID string) error {
	jKey := fmt.Sprintf("jobs/%s/%s", s.ID, jobID)

	ses, err := s.LockJob(jobID)
	if err != nil {
		return err
	}

	session := s.Client.Session()
	kv := s.Client.KV()
	defer session.Destroy(ses, nil)

	_, err = kv.DeleteTree(jKey, nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scheduler) Stop() {
	s.Mutex.Lock()
	s.SetStatus("stopped")
	s.Mutex.Unlock()
}

func (s *Scheduler) ListJobs() (consulapi.KVPairs, error) {
	qname := fmt.Sprintf("jobs/%s/", s.ID)
	//var modi uint64
	//modi = 0
	kv := s.Client.KV()
	//keys, _, err := kv.List(qname, &consulapi.QueryOptions{AllowStale: false, RequireConsistent: true, WaitIndex: modi})
	keys, _, err := kv.List(qname, nil)
	if err != nil {
		return nil, err
	}
	var keys2 consulapi.KVPairs
	for _, pair := range keys {
		parts := strings.Split(pair.Key, "/")
		if len(parts) == 3 {
			keys2 = append(keys2, pair)
		}
	}
	return keys2, nil
}

func (s *Scheduler) Start() <-chan string {
	qname := fmt.Sprintf("jobs/%s/", s.ID)
	var modi uint64
	modi = 0
	c := make(chan string)
	go func() {
		kv := s.Client.KV()
		//timeout := time.After(time.Second * 30)
		s.Mutex.Lock()
		s.SetStatus("running")
		s.Mutex.Unlock()

		for i := 0; s.GetStatus() == "running"; i++ {
			//log.Printf("Scheduler job iteration %d", i)
			keys, _, err := kv.List(qname, &consulapi.QueryOptions{AllowStale: false, RequireConsistent: true, WaitIndex: modi})
			if err != nil {
				log.Println(err)
				//s.SetStatus("Error")
			}

			for _, a := range keys {
				if a.ModifyIndex > modi {
					modi = a.ModifyIndex
				}

				if a.Key == qname {
					continue
				}
				parts := strings.Split(a.Key, "/")
				if len(parts) == 3 {
					//log.Printf("Trying to execute %s\n", parts[2])
					go s.RunJob(parts[2])
				}
			}
			/*
				select {
				case <-timeout:
					s.Status = "timeout"
					log.Println("Scheduler timeout")
					c <- s.Name
				}
			*/
		}
		log.Printf("<<<<<<<<<<<<<<<<<<<<<<< %s\n", s.GetStatus())
		c <- "End"
	}()
	return c
}

type ExecutionRun struct {
	ID           string
	JobId        string
	Node         string
	Status       string
	Output       string
	OutputErrors string
	StartTime    int64
	EndTime      int64
	StartTimeStr string
	EndTimeStr   string
	ExitErrors   string
	Session      string
}

type Job struct {
	ID, Name, Command, Output, OutputErrors string
	Status                                  string
	Type                                    string // default is "shell"
	NoWait                                  bool
	StartTime                               int64
	EndTime                                 int64
	StartTimeStr                            string
	EndTimeStr                              string
	ExecutionNode                           string
	Timeout                                 time.Duration //Timeout in seconds
	ExitErrors                              string
	LogOutput                               bool
	AllNodes                                bool
	TargetNodes                             []string
	MaxInstances                            int
	MinInstances                            int
	MaxInstancesPerNode                     int
	MaxRetries                              int
	CheckCommand                            string
	CheckInterval                           string
	StopCommand                             string
	Session                                 string
	runs                                    []ExecutionRun
}

func Connect() *consulapi.Client {
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}

	return client
}

func main() {
	app := cli.NewApp()
	app.Name = "orkestrator"
	app.Usage = "orchestrate consul cluster!"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "scheduler",
			Value: "main_scheduler",
			Usage: "scheduler name",
		},
		cli.StringFlag{
			Name:  "node",
			Value: "",
			Usage: "node name",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:      "version",
			ShortName: "v",
			Usage:     "orchestrator version",
			Action: func(c *cli.Context) {
				fmt.Println(app.Version)
			},
		},
		{
			Name:      "start",
			ShortName: "s",
			Usage:     "start scheduler",
			Action: func(c *cli.Context) {
				client := Connect()
				sche := NewScheduler(c.GlobalString("scheduler"), client, c.GlobalString("node"))
				if sche != nil {
					log.Printf("Starting orkestrator scheduler %s with node %s ...\n", c.GlobalString("scheduler"), c.GlobalString("node"))
					ch := sche.Start()
					<-ch
				}
			},
		},
		{
			Name:      "stop",
			ShortName: "t",
			Usage:     "stop scheduler",
			Action: func(c *cli.Context) {
				client := Connect()
				sche := NewScheduler(c.GlobalString("scheduler"), client, c.GlobalString("node"))
				if sche != nil {
					log.Printf("Stoping orkestrator scheduler %s with node %s ...\n", c.GlobalString("scheduler"), c.GlobalString("node"))
					sche.Stop()
				}
			},
		},
		{
			Name:      "addjob",
			ShortName: "a",
			Usage:     "add job",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "id",
					Value: func() string { s, _ := uuid.NewV4(); return s.String() }(),
					Usage: "job id",
				},
				cli.StringFlag{
					Name:  "command",
					Value: "echo hello world",
					Usage: "job command",
				},
				cli.StringFlag{
					Name:  "check",
					Value: "",
					Usage: "check command",
				},
				cli.StringFlag{
					Name:  "check_interval",
					Value: "30s",
					Usage: "check interval (ex: 2s)",
				},
				cli.BoolFlag{
					Name:  "log",
					Usage: "Log job output",
				},
				cli.StringFlag{
					Name:  "timeout",
					Value: "30s",
					Usage: "job timeout (ex: 30s)",
				},
			},
			Action: func(c *cli.Context) {
				client := Connect()
				sche := NewScheduler(c.GlobalString("scheduler"), client, c.GlobalString("node"))
				if sche != nil {
					log.Printf("Adding job to orkestrator scheduler %s...\n", c.GlobalString("scheduler"))
					var job Job
					job.ID = c.String("id")
					job.Name = c.String("id")
					job.Command = c.String("command")
					job.LogOutput = c.Bool("log")
					job.CheckCommand = c.String("check")
					job.CheckInterval = c.String("check_interval")
					dur, err := time.ParseDuration(c.String("timeout"))
					if err != nil {
						log.Fatal("invalid timeout")
					}
					job.Timeout = dur
					err = sche.AddJob(&job)
					if err != nil {
						log.Println(err)
					}
				}
			},
		},
	}
	app.Run(os.Args)
}

/* SCRATCH CODE TO DELETE

client, err := consulapi.NewClient(consulapi.DefaultConfig())
if err != nil {
	panic(err)
}

kv := client.KV()
session := client.Session()
agent := client.Agent()

err = agent.CheckRegister(&consulapi.AgentCheckRegistration{"blac", "blac", "", consulapi.AgentServiceCheck{TTL: "10s"}})
if err != nil {
	panic(err)
}
ses, _, err := session.Create(&consulapi.SessionEntry{Checks: []string{"blac"}}, nil)
if err != nil {
	panic(err)
}

ses2, _, err := session.CreateNoChecks(nil, nil)
if err != nil {
	panic(err)
}

k := &consulapi.KVPair{Key: "bla1", Value: []byte("Hola que tal"), Session: ses}
res, _, err := kv.Acquire(k, nil)
log.Printf("%v\n", res)
k.Session = ses2

res, _, err = kv.Acquire(k, nil)
log.Printf("%v\n", res)

k2, _, err := kv.Get("a/b/c", nil)
if k2 == nil {
	k2 = &consulapi.KVPair{Key: "a/b/c", Value: []byte("a taylor is rich")}
}
res, _, err = kv.CAS(k2, nil)
if err != nil {
	panic(err)
}
log.Printf("%v\n", res)
res, _, err = kv.CAS(k2, nil)
if err != nil {
	panic(err)
}
log.Printf("%v\n", res)
*/
