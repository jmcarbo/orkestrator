package orkestrator

import (
	//	log "github.com/Sirupsen/logrus"
	"fmt"
	"github.com/armon/consul-api"
)

type Orkestrator struct {
	Name string

	client *consulapi.Client
  session string
}

func NewOrkestrator(name, node string) *Orkestrator {
	if name == "" {
		name = "main"
	}
	ork := &Orkestrator{Name: name, client: Connect()}

	if node == "" {
		node, _ =  ork.client.Agent().NodeName()
	}

	oKey := fmt.Sprintf("orkestrator/%s", name)
	kvp, _, err := ork.Client().KV().Get(oKey, nil)
	if err != nil {
		return nil
	}
  if kvp == nil {
    kvp =&consulapi.KVPair{Key: oKey}
    _, err = ork.Client().KV().Put(kvp, nil)
    if err != nil {
      return nil
    }
  }

	oKey = fmt.Sprintf("orkestrator/%s/nodes/%s", name, node)
	kvp, _, err = ork.Client().KV().Get(oKey, nil)
	if err != nil {
		return nil
	}
  if kvp == nil {
    _, err := ork.Client().KV().Put(&consulapi.KVPair{Key: oKey, Value: []byte("stopped")}, nil)
    if err != nil {
      return nil
    }
  } else {
    // Node already taken
    if kvp.Session != "" {
      return nil
    }
  }
	return ork
}

func (ork *Orkestrator) Client() *consulapi.Client {
	return ork.client
}

func (ork *Orkestrator) Start() bool {

	return true
}
