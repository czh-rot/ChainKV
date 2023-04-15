package memdb

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/testutil"
	"testing"
)
type ANEWDB struct {
	p *DB
}
func TestMemDB(t *testing.T) {
	testutil.RunSuite(t, "MemDB Suite")
	fmt.Println("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOH!")
	fmt.Println("Two")
}
//var  key2=[]byte{0,1}
//var  value2=[]byte{1,2}
//func (b *ANEWDB) TestPG(key []byte,value []byte) {
//	b.p.Put(key2, value2)
//	A,_:=b.p.Get(key2)
//	fmt.Println(A)
//}