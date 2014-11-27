package orkestrator

import (
	//	log "github.com/Sirupsen/logrus"
	"fmt"
	"github.com/armon/consul-api"
)

type Orkestrator struct {
	Name string

	client *consulapi.Client
}

func NewOrkestrator(name string) *Orkestrator {
	if name == "" {
		name = "main"
	}
	ork := &Orkestrator{Name: name, client: Connect()}

	oKey := fmt.Sprintf("orkestrator/%s", name)
	_, err := ork.Client().KV().Put(&consulapi.KVPair{Key: oKey, Value: []byte("stopped")}, nil)
	if err != nil {
		return nil
	}

	return ork
}

func (ork *Orkestrator) Client() *consulapi.Client {
	return ork.client
}
