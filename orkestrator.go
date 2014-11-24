package main

import (
  "io"
  "bytes"
  "io/ioutil"
  "strings"
  "time"
  "sync"
  "log"
  "fmt"
  "errors"
  "encoding/json"
  "github.com/armon/consul-api"
  "github.com/nu7hatch/gouuid"
  "github.com/codeskyblue/go-sh"
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
  ID string
  Name string
  Policy string
  Client *consulapi.Client
  Status string
  Mutex sync.Mutex
}

func NewScheduler(name string, client *consulapi.Client) *Scheduler {
  var m sync.Mutex
  return &Scheduler{ ID: name, Name: name, Policy: "", Client: client, Mutex: m, Status: "stopped" }
}

func (s *Scheduler)AddJob(job *Job) error {
  jKey := fmt.Sprintf("jobs/%s/%s", s.ID, job.ID) 
  kv := s.Client.KV()
  cjkv, _, _ := kv.Get(jKey, nil)
  if cjkv != nil {
    return errors.New("JobExists")
  }

  b, err := json.Marshal(job)
  if err != nil {
    return err
  }

  jkv := &consulapi.KVPair{ Key: jKey, Value: b}
  _, err = kv.Put(jkv, nil)
  if err != nil {
    return err
  }


  jKey = fmt.Sprintf("jobs/%s/%s/runs", s.ID, job.ID) 
  jkv = &consulapi.KVPair{ Key: jKey}
  _, err = kv.Put(jkv, nil)
  if err != nil {
    return err
  }

  return nil
}

func (s *Scheduler)GetJobKV(jobID string) (*consulapi.KVPair, error) {
  jKey := fmt.Sprintf("jobs/%s/%s", s.ID, jobID)
  kv :=s.Client.KV()
  jkv, _, err := kv.Get(jKey, nil)
  if err != nil {
    return nil, err
  }

  return jkv, nil
}

func (s *Scheduler) AgentName() string  {
  agent :=s.Client.Agent()
  name, err:= agent.NodeName()
  if err != nil {
    return ""
  }
  return name
}

func (s *Scheduler) updateCheck(check string)  {
  agent :=s.Client.Agent()
  for {
    time.Sleep(time.Second*8)
    err := agent.PassTTL(check, "")
    if err != nil {
      break
    }
  }
}

func (s *Scheduler)LockJob(jobID string) (string, error) {
  jKey := fmt.Sprintf("jobs/%s/%s", s.ID, jobID)
  kv :=s.Client.KV()
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
    return "", err
  }

  go s.updateCheck(uid.String())

  ses, _, err:= session.Create(&consulapi.SessionEntry{Checks: []string{ uid.String() }},nil)
  //ses, _, err:= session.CreateNoChecks(nil,nil)
  if err != nil {
    return "", err
  }

  jkv.Session = ses
  res, _, err := kv.Acquire(jkv,nil)
  if err != nil {
    return "", err
  }

  if res == false {
    return "", errors.New("Can't lock job")
  }

  return ses, nil
}


func (s *Scheduler)UnlockJob(jobID string) error {
  jKey := fmt.Sprintf("jobs/%s/%s", s.ID, jobID)
  kv :=s.Client.KV()
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
  res, _, err := kv.Release(jkv,nil)
  if err != nil {
    return err
  }

  if res == false {
    return errors.New("Can't unlock job")
  }

  session :=s.Client.Session()
  sesinfo, _, err := session.Info(sess, nil)
  if err != nil {
    return err
  }
  for _, sic := range sesinfo.Checks {
    agent := s.Client.Agent()
    agent.CheckDeregister(sic)
  }
  _, err = session.Destroy(sess, nil)
  if err != nil {
    return err
  }

  return nil
}

func (s *Scheduler)decodeJob(jobid string) (Job, error) {
  var j Job
  jkv, err := s.GetJobKV(jobid) 
  if err != nil {
    return j,err
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
  return j, nil
}

func (s *Scheduler)RunJob(jobID string) error {
  _, err := s.LockJob(jobID)
  if err != nil {
    return err
  }

  log.Printf("Executing **** %s\n", jobID)
  job, err := s.decodeJob(jobID)
  job.StartTime = time.Now().UnixNano()
  job.StartTimeStr = fmt.Sprintln(time.Now())
  job.ExecutionNode = s.AgentName() 
  if err != nil {
    log.Println("Error decoding job json")
    job.Output = "Error decoding job json"
  } else {
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
      if string(out) != "" {
        log.Printf("Output job %s **** %s\n", job.ID, string(out))
      }
      if string(stderr) != "" {
        log.Printf("Error job %s **** %s\n", job.ID, string(stderr))
      }
      if err != nil {
        log.Printf("Exit error %v", err)
        job.ExitErrors = fmt.Sprintf("%v", err)
      }
      job.Output = string(out)
      job.OutputErrors = string(stderr)
    }
  } 
  job.EndTime = time.Now().UnixNano() 
  job.EndTimeStr = fmt.Sprintln(time.Now())


  err = s.UnlockJob(jobID)
  if err != nil {
    return err
  }
  return nil
}

func (s *Scheduler)DeleteJob(jobID string) error {
  jKey := fmt.Sprintf("jobs/%s/%s", s.ID, jobID)

  ses, err := s.LockJob(jobID)
  if err != nil {
    return err
  }

  session := s.Client.Session()
  kv :=s.Client.KV()
  defer session.Destroy(ses, nil)

  _, err = kv.DeleteTree(jKey, nil)
  if err != nil {
    return err
  }

  return nil
}

func (s *Scheduler)Stop() {
    s.Mutex.Lock()
    s.Status = "stopped"
    s.Mutex.Unlock()
}

func (s *Scheduler)ListJobs() (consulapi.KVPairs, error) {
  qname := fmt.Sprintf("jobs/%s", s.ID)
  var modi uint64
  modi = 0
  kv :=s.Client.KV()
  keys, _, err := kv.List(qname, &consulapi.QueryOptions{ AllowStale: false, RequireConsistent: true, WaitIndex: modi })
  if err != nil {
    return nil, err
  }
  var keys2 consulapi.KVPairs
  for _, pair := range keys {
    parts:=strings.Split(pair.Key, "/")
    if len(parts) == 3 {
      keys2=append(keys2, pair)
    } 
  }
  return keys2, nil
}

func (s *Scheduler)Start() <-chan string {
  qname := fmt.Sprintf("jobs/%s", s.ID)
  var modi uint64
  modi = 0
  c := make(chan string)
  go func(){
    kv :=s.Client.KV()
    timeout := time.After(time.Second*30)
    s.Mutex.Lock()
    s.Status = "running"
    s.Mutex.Unlock()
    for s.Status == "running" {
      keys, _, err := kv.List(qname, &consulapi.QueryOptions{ AllowStale: false, RequireConsistent: true, WaitIndex: modi })
      if err != nil {
        s.Status = "Error"
      }

      for _,a := range keys {
        if a.ModifyIndex > modi {
          modi = a.ModifyIndex
        }

        if a.Key == qname {
          continue
        }
      }

      select {
        case <-timeout:
          s.Status = "timeout"
          log.Println("Scheduler timeout")
          c <- s.Name
      }
    }
  }()

  return c
}

type ExecutionRun struct {
  ID string
  Status string
  Output string
}

type Job struct {
  ID, Name, Command, Output, OutputErrors string
  Status string
  Type string // default is "shell"
  NoWait bool
  StartTime int64
  EndTime int64
  StartTimeStr string
  EndTimeStr string
  ExecutionNode string
  Timeout time.Duration //Timeout in seconds
  ExitErrors string
}

func Connect() *consulapi.Client {
  client, err := consulapi.NewClient(consulapi.DefaultConfig())
  if err != nil {
    log.Fatal(err)
  }


  return client
}

func main(){
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
  ses, _, err:= session.Create(&consulapi.SessionEntry{Checks: []string{"blac"}},nil)
  if err != nil {
    panic(err)
  }

  ses2, _, err:= session.CreateNoChecks(nil,nil)
  if err != nil {
    panic(err)
  }

  k := &consulapi.KVPair{ Key: "bla1", Value: []byte("Hola que tal"), Session: ses}
  res, _, err := kv.Acquire(k,nil)
  log.Printf("%v\n", res)
  k.Session = ses2

  res, _, err = kv.Acquire(k,nil)
  log.Printf("%v\n", res)

  k2,_,err:=kv.Get("a/b/c",nil)
  if k2 == nil {
    k2=&consulapi.KVPair{ Key: "a/b/c", Value: []byte("a taylor is rich")}
  }
  res, _, err = kv.CAS(k2,nil)
  if err != nil {
    panic(err)
  }
  log.Printf("%v\n", res)
  res, _, err = kv.CAS(k2, nil)
  if err != nil {
    panic(err)
  }
  log.Printf("%v\n", res)

}
