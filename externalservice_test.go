package orkestrator

import (
	. "github.com/franela/goblin"
	"testing"
  "time"
  "io/ioutil"
  "os"
  "os/exec"
)

func Test(t *testing.T) {

	g := Goblin(t, "-goblin.timeout=20s")
	g.Describe("externalservice", func() {
    g.Before(func(){
			client := Connect()
      DestroyAllExternalServices(client)
    })
		g.It("can be created", func() {
			client := Connect()
			externalService := NewExternalService(client, "testlock1", "node1", "localhost", 80, "ping -c 2 localhost")
			g.Assert(externalService != nil).IsTrue()
		})
		g.It("can be created from consul", func() {
			client := Connect()
			externalService := NewExternalService(client, "testlock1", "node1", "localhost", 80, "ping -c 2 localhost")
			g.Assert(externalService != nil).IsTrue()
			externalService2 := NewExternalServiceFromConsul(client, "testlock1", "node1")
      g.Assert(externalService).Equal(externalService2)
		})
		g.It("can be registered", func() {
			client := Connect()
			es := NewExternalService(client, "testlock1", "node1", "localhost", 80, "ping -c 2 localhost")
      err:=es.Register()
			g.Assert(err).Equal(nil)
		})

		g.It("can be registered and healthy", func() {
			client := Connect()
			es := NewExternalService(client, "testlock3", "node1", "localhost", 80, "ping -c 2 localhost")
      es.SetCheckInterval("1s")
      err:=es.Register()
			g.Assert(err).Equal(nil)
      time.Sleep(time.Second*3)
      health:= es.IsHealthy()
			g.Assert(health).IsTrue()
		})

		g.It("can be registered and ill", func() {
			client := Connect()
			es := NewExternalService(client, "testlock11", "node1", "localhost", 80, "ping -c 2 ost")
      es.SetCheckInterval("1s")
      err:=es.Register()
			g.Assert(err).Equal(nil)
      time.Sleep(time.Second*3)
      health:= es.IsHealthy()
			g.Assert(health).IsFalse()
		})

		g.It("can be unregistered", func() {
			client := Connect()
			es := NewExternalService(client, "testlock1", "node1", "localhost", 80, "ping -c 2 localhost")
      err:=es.Register()
			g.Assert(err).Equal(nil)
      err=es.Unregister()
			g.Assert(err).Equal(nil)
		})
	})

	g.Describe("externalservicewatcher", func() {
    g.Before(func(){
			client := Connect()
      DestroyAllExternalServices(client)
    })
		g.It("can create watcher", func() {
			client := Connect()
			esw := NewExternalServiceWatcher(client, "a")
			g.Assert(esw!=nil).IsTrue()
      esw.Destroy()
		})

		g.It("can run watcher", func() {
			client := Connect()
			esw := NewExternalServiceWatcher(client, "a")
      err := esw.Run()
      time.Sleep(time.Second*1)
			g.Assert(err==nil).IsTrue()
      esw.Destroy()
		})

		g.It("can run and stop watcher", func() {
			client := Connect()
			esw := NewExternalServiceWatcher(client, "node1")
      esw.Run()
      time.Sleep(time.Second*1)
      err:= esw.Stop()
			g.Assert(err==nil).IsTrue()
      esw.Destroy()
		})

		g.It("can run with check passing", func() {
			client := Connect()
			esw := NewExternalServiceWatcher(client, "node10")
      esw.Run()
			es := NewExternalService(client, "testlock1", "node10", "localhost", 80, "ping -c 2 localhost")
      es.Register()
      time.Sleep(time.Second*5)
      //err:= esw.Stop()
			g.Assert(es.IsHealthy()).IsTrue()
			g.Assert(es.IsActive()).IsTrue()
      esw.Destroy()
      es.Unregister()
		})

		g.It("can activate service create with curl", func() {
			client := Connect()
			esw := NewExternalServiceWatcher(client, "node20")
      esw.Run()
      err:=exec.Command("curl", "-XPUT", "http://localhost:8500/v1/kv/ExternalServices/node20/aservice", "-d { \"Address\":\"localhost\",\"Port\":80,\"Interval\":\"1s\",\"Command\":\"ping -c 1 localhost\",\"TargetState\":\"running\",\"State\":\"\" }" ).Run()
      if err != nil {
        t.Log(err)
      }
      time.Sleep(time.Second*5)
      //err:= esw.Stop()
      es := NewExternalServiceFromConsul(client, "aservice", "node20")
      g.Assert(es!=nil).IsTrue()
			g.Assert(es.IsHealthy()).IsTrue()
			g.Assert(es.IsActive()).IsTrue()
      esw.Destroy()
      es.Unregister()
		})

		g.It("can run with check bouncing", func() {
			client := Connect()
			esw := NewExternalServiceWatcher(client, "node11")
      esw.Run()
      ioutil.WriteFile("test.txt", []byte("blabla"), 0666)
			es := NewExternalService(client, "testlock1", "node11", "localhost", 80, "cat test.txt || if [[ \"$?\" == 1 ]]; then exit 2; fi")
      es.SetCheckInterval("1s")
      es.Register()
      time.Sleep(time.Second*3)
      //err:= esw.Stop()
			g.Assert(es.IsHealthy()).IsTrue()
			g.Assert(es.IsActive()).IsTrue()
      os.Remove("test.txt")
      time.Sleep(time.Second*10)
			g.Assert(es.IsHealthy()).IsFalse()
			g.Assert(es.IsActive()).IsFalse()
      esw.Destroy()
      es.Unregister()
		})

		g.It("cannot run two watchers on same node", func() {
			client := Connect()
			esw := NewExternalServiceWatcher(client, "b")
      esw.Run()
      time.Sleep(time.Second*1)
			esw2 := NewExternalServiceWatcher(client, "b")
      err:=esw2.Run()
			g.Assert(err!=nil).IsTrue()
      esw.Destroy()
      time.Sleep(time.Second*1)
      err=esw2.Run()
			g.Assert(err==nil).IsTrue()
      esw2.Destroy()
    })
	})
}
