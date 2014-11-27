package orkestrator

import (
	. "github.com/franela/goblin"
	"testing"
)

func Test(t *testing.T) {

	g := Goblin(t)
	g.Describe("orkestrator", func() {
		g.It("must be instantiated", func() {
			ork := NewOrkestrator("", "")
			g.Assert(ork != nil).IsTrue()
		})

		g.It("must register himself with consul", func() {
			ork := NewOrkestrator("", "")
			kvp, _, err := ork.Client().KV().Get("orkestrator/main", nil)
			g.Assert(err).Equal(nil)
			g.Assert(kvp.Key).Equal("orkestrator/main")
			g.Assert(kvp.Value).Equal([]byte("stopped"))
		})
	})

}
