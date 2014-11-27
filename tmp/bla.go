package main

import (
	"github.com/armon/consul-api"
	//"github.com/nu7hatch/gouuid"
	//"time"
  "log"
)

func Connect() *consulapi.Client {
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}

	return client
}

func main() () {
	client := Connect()
	//agent := client.Agent()
  //catalog := client.Catalog()
  kv := client.KV()
	//uid, _ := uuid.NewV4()


  /*
  err := agent.CheckRegister(&consulapi.AgentCheckRegistration{uid.String(), uid.String(), "",
	  consulapi.AgentServiceCheck{Interval: "5s", Script: "ping -c 3 localhost"}})
	if err != nil {
		
	}
	//TODO convert job.CheckInterval to integer
	time.Sleep(time.Second * 5)
  ac, _ :=agent.Checks()
	uis, _ := uuid.NewV4()

  _, err = catalog.Register(&consulapi.CatalogRegistration{ Node: "google", Address: "127.0.1.1", 
  Service: &consulapi.AgentService{ID:uis.String(),  Service: uis.String(), Port: 80}, Check: ac[uid.String()]}, nil)

  */
  var index uint64
  index = 0
  for {
    kp, _, _ := kv.List("bla", &consulapi.QueryOptions{ WaitIndex: index })
    for _, p := range kp {
      log.Printf("%s -- %s", p.Key, p.Value)
      if p.ModifyIndex > index { index = p.ModifyIndex }
    }
  }
}
