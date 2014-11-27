package orkestrator

import (
  "testing"
	. "github.com/franela/goblin"
)

func Test(t *testing.T) {

	g := Goblin(t)
	g.Describe("lock", func() {
    g.It("create and lock", func(){
      client := Connect()
      lock := NewLock(client, "testlock") 
      g.Assert(lock!=nil).IsTrue()
      g.Assert(lock.IsLeader()).IsTrue()
    })

    g.It("can be unlock", func(){
      client := Connect()
      lock := NewLock(client, "testlock") 
      err:=lock.Unlock()
      g.Assert(err==nil).IsTrue()
      g.Assert(lock.IsUnlocked()).IsTrue()
    })
  })
}
