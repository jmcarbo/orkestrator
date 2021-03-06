package orkestrator

import (
	//"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/armon/consul-api"
	//"github.com/nu7hatch/gouuid"
	"time"
  "sync"
  "encoding/json"
  "strings"
  "io"
)

type ExternalService struct {
	service              string
	node              string
	client           *consulapi.Client
  definition      *ExternalServiceDefinition
}

type ExternalServiceDefinition struct {
  Address           string
  Port              int
  Command           string
  State             string
  Interval          string
  TargetState       string
}

func NewExternalService(client *consulapi.Client, service, node, address string, port int, command string) *ExternalService {
  es := &ExternalService{ service: service, node: node, 
    definition: &ExternalServiceDefinition{Address: address, Port: port, Command: command, TargetState:"stopped", Interval: "10s"}, client: client }

  esKey:=fmt.Sprintf("ExternalServices/%s/%s", es.node, es.service)
  b, _ := json.Marshal(es.definition)
  _, err := es.client.KV().Put(&consulapi.KVPair{Key: esKey, Value: b }, nil)
  if err != nil {
    return nil
  }
	return es
}

func NewExternalServiceFromConsul(client *consulapi.Client, service, node string) *ExternalService {

  esKey:=fmt.Sprintf("ExternalServices/%s/%s", node, service)
  kvp, _, err := client.KV().Get(esKey, nil)
  if err != nil {
    //log.Errorf("Error getting %s", esKey)
    return nil
  }
  if kvp == nil {
    //log.Errorf("No key %s", esKey)
    return nil
  }

  var esd ExternalServiceDefinition
	dec := json.NewDecoder(strings.NewReader(string(kvp.Value)))
	if err := dec.Decode(&esd); err == io.EOF {
    //log.Errorf("Decoding %s", string(kvp.Value))
		return nil
	} else if err != nil {
    log.Errorf("Decoding %s", string(kvp.Value))
		return nil
	}
  es := &ExternalService{ service: service, node: node, definition: &esd, client: client }
	return es
}

func (es *ExternalService) SetCheckInterval(interval string) error {
  es.definition.Interval = interval
  return nil
}

func (es *ExternalService) Save() error {
  esKey:=fmt.Sprintf("ExternalServices/%s/%s", es.node, es.service)
  b, _ := json.Marshal(es.definition)
  _, err := es.client.KV().Put(&consulapi.KVPair{Key: esKey, Value: b }, nil)
  if err != nil {
    return err
  }
  return nil
}

func (es *ExternalService) Register() error {
  es.definition.TargetState="running"
  es.Save()
  _, err := es.client.Catalog().Register(&consulapi.CatalogRegistration{Node: es.node, Address: es.definition.Address,
    Service: &consulapi.AgentService { ID: es.service, Service: es.service, Port: es.definition.Port }}, nil)
  if err != nil {
    return err
  }

  if !es.CheckExists(){
    checkName := fmt.Sprintf("check:%s:%s", es.service, es.node)
    err = es.client.Agent().CheckRegister(&consulapi.AgentCheckRegistration{checkName, checkName, "",
      consulapi.AgentServiceCheck{Interval: es.definition.Interval, Script: es.definition.Command}})
    if err != nil {
      return err
    }
  }
  return nil
}

func (es *ExternalService) IsActive() bool {
  cs, _, err := es.client.Catalog().Service(es.service, "", nil)
  if err != nil {
    return false
  }
  if cs == nil {
    return false
  }
  //log.Infof("%#v", cs)
  //log.Infof("%#v", cs[0])
  //log.Infof("%s", es.service)

  for _, s := range cs {
    if s.ServiceName == es.service && s.Node == es.node {
      return true
    }
  }
  //log.Error("Service not found")
  return false
}

func (es *ExternalService) UnregisterService() error {
  _, err := es.client.Catalog().Deregister(&consulapi.CatalogDeregistration{Node: es.node, Address: es.definition.Address, ServiceID: es.service }, nil)
  if err != nil {
    return err
  }
  return nil
}

func (es *ExternalService) Unregister() error {
  es.definition.TargetState="running"
  es.Save()

  _, err := es.client.Catalog().Deregister(&consulapi.CatalogDeregistration{Node: es.node, Address: es.definition.Address, ServiceID: es.service }, nil)
  if err != nil {
    return nil
  }
	err =	es.client.Agent().CheckDeregister(es.service)
  if err != nil {
    return nil
  }
  return nil
}

func (es *ExternalService) CheckExists() bool {
  checks, err:=es.client.Agent().Checks()
  if err != nil {
    return false
  }
  checkName := fmt.Sprintf("check:%s:%s", es.service, es.node)
  if checks != nil && checks[checkName]!=nil {
    return true
  }
  return false
}

func (es *ExternalService) IsHealthy() bool {
  checks, err:=es.client.Agent().Checks()
  if err != nil {
    return false
  }
  checkName := fmt.Sprintf("check:%s:%s", es.service, es.node)
  if checks != nil && checks[checkName]!=nil && checks[checkName].Status == "passing" {
    return true
  }
  return false
}

func (es *ExternalService) CheckStatus() string {
  checks, err:=es.client.Agent().Checks()
  if err != nil {
    return "unknown"
  }
  checkName := fmt.Sprintf("check:%s:%s", es.service, es.node)
  if checks != nil && checks[checkName]!=nil {
    return checks[checkName].Status
  }
  return "unknown"
}

func DestroyAllExternalServices(client *consulapi.Client) error {
  _, err := client.KV().DeleteTree("ExternalServices", nil)
  return err
}

type ExternalServiceWatcher struct {
  node string
	client           *consulapi.Client
  state string
  slock   sync.Mutex
  kvlock          *Lock
	stopCh           chan struct{}
	doneCh           chan struct{}
}

func NewExternalServiceWatcher(client *consulapi.Client, node string) *ExternalServiceWatcher {
  esw := &ExternalServiceWatcher{ client:client, node: node }
  esw.setState("stopped")
  esKey:=fmt.Sprintf("ExternalServicesWatchers/%s", esw.node)
  esw.kvlock = NewLock(client, esKey)
  if esw.kvlock == nil {
    return nil
  }
	esw.doneCh = make(chan struct{})
	esw.stopCh = make(chan struct{})
  return esw
}

func (esw *ExternalServiceWatcher) setState(state string) {
  esw.slock.Lock()
  esw.state = state
  esw.slock.Unlock()
}

func (esw *ExternalServiceWatcher) Run() error {
  err:= esw.kvlock.Lock(nil)
  if err != nil {
    return err
  }

  go func(){
    var modi uint64
    modi = 0
    qname := fmt.Sprintf("ExternalServices/%s", esw.node)
    dur, err := time.ParseDuration("3s")
    if err != nil {
      esw.setState("stopped")
      return 
    }
    for {
      keys, qm, err := esw.client.KV().List(qname, &consulapi.QueryOptions{AllowStale: false, RequireConsistent: true, WaitTime: dur, WaitIndex: modi})
      if err != nil {
        return
      }

      for _, a := range keys {
        parts := strings.Split(a.Key, "/")
        if len(parts) == 3 {
          node := parts[1]
          service := parts[2]
          es := NewExternalServiceFromConsul(esw.client, service, node)
          if es != nil {
            if es.definition.TargetState == "running" {
              if !es.CheckExists() {
                es.Register()
              }
            }
            if es.definition.TargetState == "stopped" {
              es.Unregister()
            }
            if es.definition.TargetState == "deleted" {
              es.Unregister()
            }
          }
        }
      }

      modi = qm.LastIndex
      select {
        case <-esw.stopCh:
          return
        default:
          if !esw.kvlock.IsLeader() {
            return
          }
      }
    }
  }()

  go func(){
    esw.setState("running")
    var modi uint64
    modi = 0
    //qname := fmt.Sprintf("ExternalServices/%s", esw.node)
    dur, err := time.ParseDuration("3s")
    if err != nil {
      esw.setState("stopped")
      return 
    }
    for {
      //keys, qm, err := esw.client.KV().List("ExternalServices", &consulapi.QueryOptions{AllowStale: false, RequireConsistent: true, WaitTime: dur, WaitIndex: modi})
      hcks,qm,err:=esw.client.Health().State("any", &consulapi.QueryOptions{AllowStale: false, RequireConsistent: true, WaitTime: dur, WaitIndex: modi})
      if err != nil {
        return
      }

      for _, a := range hcks {
        var service, node string
        parts := strings.Split(a.Name, ":")
        if len(parts)==3 && parts[0]=="check" {
          service = parts[1]
          node = parts[2]
          if node == esw.node {
            //log.Infof("Getting %s x--------> %s", service, node)
            es := NewExternalServiceFromConsul(esw.client, service, node)
            if es != nil {
              if a.Status == "passing" {
                es.Register()
                //log.Infof("Registering %s --------> %s ----> %s ----> %s", service, node, esw.node, a.Status)
              }
              if a.Status == "critical" {
                err:=es.UnregisterService()
                if err != nil {
                  log.Error("unregistering service %s", service)
                }
                //log.Infof("UnRegistering %s --------> %s ----> %s ----> %s", service, node, esw.node, a.Status)
              }
            } else {
	            esw.client.Agent().CheckDeregister(a.Name)
            }

          }
        }
      }

      modi = qm.LastIndex
      select {
        case <-esw.stopCh:
          return
        default:
          if !esw.kvlock.IsLeader() {
            return
          }
      }
    }
  }()
  return nil
}

func (esw *ExternalServiceWatcher) Stop() error {
  err:=esw.kvlock.Unlock()
  if err != nil {
    return err
  }
  return nil
}


func (esw *ExternalServiceWatcher) Destroy() error {
  err:=esw.Stop()
  if err != nil {
    return err
  }
  err=esw.kvlock.Destroy()
  if err != nil {
    return err
  }
  /*
  esKey:=fmt.Sprintf("ExternalServicesWatchers/%s", esw.node)
  _, err = esw.client.KV().Delete(esKey, nil)
  if err != nil {
    return err
  }
  */
  return nil
}
