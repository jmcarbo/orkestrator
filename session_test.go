package orkestrator

import (
	. "github.com/franela/goblin"
	"testing"
)

func Test(t *testing.T) {

	g := Goblin(t)
	g.Describe("session", func() {
		g.It("create and start", func() {
			client := Connect()
			sess := NewSession(client, "")
			g.Assert(sess != nil).IsTrue()
			g.Assert(sess.IsHealthy()).IsTrue()
		})

		g.It("can be destroyed", func() {
			client := Connect()
			sess := NewSession(client, "")
			err := sess.Destroy()
			g.Assert(err == nil).IsTrue()
			g.Assert(sess.IsDestroyed()).IsTrue()
		})
	})
}
