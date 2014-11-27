package orkestrator

import (
	. "github.com/franela/goblin"
	"testing"
)

func Test(t *testing.T) {
	g := Goblin(t)
	g.Describe("worker", func() {
		g.It("can be instantiated", func() {
			w := NewWorker()
			g.Assert(w == nil).IsFalse()
		})
	})
}
