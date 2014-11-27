package orkestrator

import (
	. "github.com/franela/goblin"
	"testing"
  "fmt"
)

func Test(t *testing.T) {

	g := Goblin(t)
	g.Describe("orkestrator", func() {
		g.It("must be instantiated", func() {
			ork := NewOrkestrator("", "")
			g.Assert(ork != nil).IsTrue()
		})

		g.It("must register himself with consul with default name and node", func() {
			ork := NewOrkestrator("", "")
      node, _ := ork.Client().Agent().NodeName()
      orkestratorName := "main"
      orkKey := fmt.Sprintf("orkestrator/%s", orkestratorName)
			kvp, _, err := ork.Client().KV().Get(orkKey, nil)
			g.Assert(err).Equal(nil)
			g.Assert(kvp.Key).Equal(orkKey)

      orkKeyNode := fmt.Sprintf("orkestrator/%s/nodes/%s", orkestratorName, node)
			kvp, _, err = ork.Client().KV().Get(orkKeyNode, nil)
			g.Assert(err).Equal(nil)
			g.Assert(kvp.Key).Equal(orkKeyNode)
			g.Assert(kvp.Value).Equal([]byte("stopped"))
		})

		g.It("must register himself with consul with custom name and node", func() {
      node := "node1"
      orkestratorName := "testorkestrator"
			ork := NewOrkestrator(orkestratorName, node)
      orkKey := fmt.Sprintf("orkestrator/%s", orkestratorName)
			kvp, _, err := ork.Client().KV().Get(orkKey, nil)
			g.Assert(err).Equal(nil)
			g.Assert(kvp.Key).Equal(orkKey)

      orkKeyNode := fmt.Sprintf("orkestrator/%s/nodes/%s", orkestratorName, node)
			kvp, _, err = ork.Client().KV().Get(orkKeyNode, nil)
			g.Assert(err).Equal(nil)
			g.Assert(kvp.Key).Equal(orkKeyNode)
			g.Assert(kvp.Value).Equal([]byte("stopped"))
		})

    g.It("can be started and become leader", func(){
			ork := NewOrkestrator("", "")
      g.Assert(ork.Start()).IsTrue()
    })
	})

}
