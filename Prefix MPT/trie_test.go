// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"MPT/ethdb"
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"github.com/syndtr/goleveldb/leveldb/cache"

	//"github.com/syndtr/goleveldb/leveldb/cache"
	"runtime"
	"time"

	//"encoding/hex"
	"errors"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"io/ioutil"
	"math/big"
	"math/rand"
	"os"
	"reflect"
	"testing"
)

func init() {
	spew.Config.Indent = "    "
	spew.Config.DisableMethods = false
}

// Used for testing

func newPresentp(r []byte) *Trie {
	//db, _ := ethdb.NewMemDatabase()
	//指定存储路径、writeBuffer、handle三个参数
	db,_ := ethdb.NewLDBDatabase("E:/DB/200/200T",16,50)
	trie, _ := News(r, db)
	return trie
}

func newPresentp2(r []byte) *Trie {
	//db, _ := ethdb.NewMemDatabase()
	//指定存储路径、writeBuffer、handle三个参数
	db,_ := ethdb.NewLDBDatabase("E:/DB/200/200P",16,50)
	trie, _ := News(r, db)
	return trie
}

func TestEmptyTrie(t *testing.T) {
	var trie Trie
	res := trie.Hash()
	exp := emptyRoot
	if res != common.Hash(exp) {
		t.Errorf("expected %x got %x", exp, res)
	}
}

func TestNull(t *testing.T) {
	var trie Trie
	key := make([]byte, 32)
	value := []byte("test")
	trie.Update(key, value)
	if !bytes.Equal(trie.Get(key), value) {
		t.Fatal("wrong value")
	}
}

//func TestMissingRoot(t *testing.T) {
//	db, _ := ethdb.NewMemDatabase()
//	trie, err := New(common.HexToHash("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33"), db)
//	if trie != nil {
//		t.Error("New returned non-nil trie for invalid root")
//	}
//	if _, ok := err.(*MissingNodeError); !ok {
//		t.Errorf("New returned wrong error: %v", err)
//	}
//}
//
//func TestMissingNode(t *testing.T) {
//	db, _ := ethdb.NewMemDatabase()
//	trie, _ := New(common.Hash{}, db)
//	updateString(trie, "120000", "qwerqwerqwerqwerqwerqwerqwerqwer")
//	updateString(trie, "123456", "asdfasdfasdfasdfasdfasdfasdfasdf")
//	root,_, _ := trie.Commit()
//
//	trie, _ = New(root, db)
//	_, err := trie.TryGet([]byte("120000"))
//	if err != nil {
//		t.Errorf("Unexpected error: %v", err)
//	}
//
//	trie, _ = New(root, db)
//	_, err = trie.TryGet([]byte("120099"))
//	if err != nil {
//		t.Errorf("Unexpected error: %v", err)
//	}
//
//	trie, _ = New(root, db)
//	_, err = trie.TryGet([]byte("123456"))
//	if err != nil {
//		t.Errorf("Unexpected error: %v", err)
//	}
//
//	trie, _ = New(root, db)
//	err = trie.TryUpdate([]byte("120099"), []byte("zxcvzxcvzxcvzxcvzxcvzxcvzxcvzxcv"))
//	if err != nil {
//		t.Errorf("Unexpected error: %v", err)
//	}
//
//	trie, _ = New(root, db)
//	err = trie.TryDelete([]byte("123456"))
//	if err != nil {
//		t.Errorf("Unexpected error: %v", err)
//	}
//
//	_ = db.Delete(common.FromHex("e1d943cc8f061a0c0b98162830b970395ac9315654824bf21b73b891365262f9"))
//
//	trie, _ = New(root, db)
//	_, err = trie.TryGet([]byte("120000"))
//	if _, ok := err.(*MissingNodeError); !ok {
//		t.Errorf("Wrong error: %v", err)
//	}
//
//	trie, _ = New(root, db)
//	_, err = trie.TryGet([]byte("120099"))
//	if _, ok := err.(*MissingNodeError); !ok {
//		t.Errorf("Wrong error: %v", err)
//	}
//
//	trie, _ = New(root, db)
//	_, err = trie.TryGet([]byte("123456"))
//	if err != nil {
//		t.Errorf("Unexpected error: %v", err)
//	}
//
//	trie, _ = New(root, db)
//	err = trie.TryUpdate([]byte("120099"), []byte("zxcv"))
//	if _, ok := err.(*MissingNodeError); !ok {
//		t.Errorf("Wrong error: %v", err)
//	}
//
//	trie, _ = New(root, db)
//	err = trie.TryDelete([]byte("123456"))
//	if _, ok := err.(*MissingNodeError); !ok {
//		t.Errorf("Wrong error: %v", err)
//	}
//}


func NewPresent(r []byte) *Trie {
	//db, _ := ethdb.NewMemDatabase()
	//指定存储路径、writeBuffer、handle三个参数
	db,_ := ethdb.NewLDBDatabase("/media/czh/sn/DB/1",16,1024)
	trie, _ := News(r, db)
	return trie
}
func newEmpty() *Trie {
	//db, _ := ethdb.NewMemDatabase()
	//指定存储路径、writeBuffer、handle三个参数，writeBuffer=cache/4
	db,_ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Acc",16,128)
	trie, _ := New(common.Hash{}, db)
	return trie
}
// Question:为什么会出现一条因为RLP编码长度不够而不存入数据库中？
// 猜想：当叶子很小时，直接把node放入父节点中。
func TestDB (t *testing.T){
	newEmpty()
}
func TestMyInsert(t *testing.T){
	//132 0 59 165 252 52 160 208 92 17 70 162 180 18 77 149 155 195 222 231 14 5 150 170 177 231 181 6 178 219 70 170
	trie := newEmpty()
	//读文件
	f,_:=os.Open("/media/czh/sn/record/UniqueA2.txt")
	//f2, err := os.OpenFile("E:/DB/record/UniqueA2.txt",os.O_WRONLY | os.O_CREATE | os.O_APPEND,0666)
	s := bufio.NewScanner(f)
	count:=0
	//countU:=0
	//B:=0
	rand.Seed(0)
	for s.Scan() {
		value := make([]byte,110)
		str:=s.Text()
		key, _ := hex.DecodeString(str[:])
		rand.Read(value)
		// 插入MPT,先对Key进行加密变为32，再变成hex码
		trie.Update(crypto.Keccak256(key),value)
		count++
	}
	defer f.Close()
	// 提交入库
	root,Proot, err := trie.Commit()
	//time.Sleep(5e9)
	fmt.Println("----------------------------------------分割线----------------------------------------")
	//这里之所以root为32字节，是因为只有这么长，有一个裁剪的过程
	fmt.Println(count,ethdb.I,ethdb.Size,A)
	fmt.Println("前缀前Trie树的根：",root)
	// 读的时候有用！
	fmt.Println("前缀后Tire树的根:",Proot)
	if err!=nil{
		panic("")
	}
   //root3 := []byte{16, 154, 137, 29, 83, 174, 214, 190, 39, 12, 125, 65, 0, 137, 9, 204, 24, 237, 131, 58, 150, 132, 67, 65, 170, 245, 4, 35, 252, 67, 249, 33, 134}
}
// 测试【取消/添加】前缀是否成功

func TestMyInsert2(t *testing.T){
	trie := newEmpty()
	// 提交入库
	trie.Update([]byte{1,2,3,4,5}, []byte("catcatcatcatcatcatcatcatcatcat"))
	trie.Update([]byte{1,2,3,6,7}, []byte("catcatcatcatcatcatcatcatcatcat"))
	root,Proot, err := trie.Commit()
	fmt.Println("----------------------------------------分割线----------------------------------------")
	//这里之所以root为32字节，是因为只有这么长，有一个裁剪的过程
	fmt.Println("前缀前Trie树的根：",root)
	fmt.Println("前缀后Tire树的根:",Proot)
	if err!=nil{
		panic("")
	}
}

func TestMyInsert3(t *testing.T){
	trie := newEmpty()
	// 提交入库
	trie.Update([]byte{1,2,3,4,5}, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	trie.Update([]byte{1,2,3,4,6}, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	trie.Update([]byte{1,2,3,4,7}, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	fmt.Println("----------------------------------------分割线----------------------------------------")
	root2,Proot2, err := trie.Commit()
	fmt.Println("----------------------------------------分割线----------------------------------------")
	//这里之所以root为32字节，是因为只有这么长，有一个裁剪的过程
	fmt.Println("前缀前Trie树的根：",root2,A)
	fmt.Println("前缀后Tire树的根:",Proot2,A,ethdb.I)
	if err!=nil{
		panic("")
	}

}

func TestDDDDD(t *testing.T) {
	db,_ := ethdb.NewLDBDatabase("/media/czh/sn/DB/1",16,128)
	root:=[]byte{16, 90 ,244, 113, 162, 254, 31, 109, 220, 17, 80, 166, 233, 239, 149, 154, 68, 165,72, 95, 237, 88, 193, 82, 171, 74, 203, 159, 149, 69, 58, 235, 246}
	root2:=[]byte{219, 97, 216, 117, 231, 43, 173, 92, 24, 242, 67, 180, 24, 44, 86, 169, 135, 157, 251, 3, 77, 253, 234, 104, 2, 64, 26, 21, 221, 42, 125, 167}
	v, _ :=db.Get(root)
	v1, _ :=db.Get(root2)
	fmt.Println(v,"\n",v1)
}
func TestMyGet3(t *testing.T){
	//root:=[]byte{160, 90 ,244, 113, 162, 254, 31, 109, 220, 17, 80, 166, 233, 239, 149, 154, 68, 165,72, 95, 237, 88, 193, 82, 171, 74, 203, 159, 149, 69, 58, 235, 246}
	root2:=[]byte{219, 97, 216, 117, 231, 43, 173, 92, 24, 242, 67, 180, 24, 44, 86, 169, 135, 157, 251, 3, 77, 253, 234, 104, 2, 64, 26, 21, 221, 42, 125, 167}
	trie :=NewPresent(root2)
	v:=trie.Get([]byte{1,2,3,4,5})
	v1:=trie.Get([]byte{1,2,3,4,6})
	v2:=trie.Get([]byte{1,2,3,4,7})
	fmt.Println(v1,v2,v)
}
//主要用途为测试是否能读到，且查看查询时间。
func TestMyGet(t *testing.T){
	//root:=[]byte{132, 0, 59, 165, 252, 52, 160, 208, 92, 17, 70, 162, 180, 18, 77, 149, 155, 195, 222, 231, 14, 5, 150, 170, 177, 231, 181, 6, 178, 219, 70, 170}
	root:=[]byte{240, 114, 194, 194, 17, 14, 199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	db,_ := ethdb.NewLDBDatabase("C:/DB/State",16,1024)
	//db,_ := ethdb.NewLDBDatabase("C:/DB/Double-P",16,1024)
	//tree, _ := News(root, db)
	f,_:=os.Open("F:/paper/record/UniqueA2.txt")
	s := bufio.NewScanner(f)
	index:=0
	Count:=0
	var shijian float64
	for s.Scan() {
		tree, _ := News(root, db)
		str:=s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		t1:=time.Now()
		v:=tree.Get(crypto.Keccak256(key))
		t2:=time.Now()
		if v == nil{
			index++
		}
		shijian += t2.Sub(t1).Seconds()
		Count++
		if Count == 10{
			break
		}
	}
	//index = int(trie.PtintItems())
	fmt.Println("Get耗费时间：",shijian,Count)
	fmt.Println(index,ethdb.Count,ethdb.T)
	fmt.Println("qps:",float64(1000000)/ethdb.T,float64(1000000)/shijian)
	//fmt.Println(cache.Hit, cache.Miss,float64(cache.Hit)/float64(cache.Hit+cache.Miss))
	fmt.Println(cache.HitNumber, cache.MissNumber,float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2,float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率",float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	runtime.GC()
}

func TestMyGetP(t *testing.T){
	// 有前缀
	fmt.Println("1")
	root:=[]byte{198 ,49 ,76 ,24 ,169 ,136 ,93 ,195 ,207 ,6 ,149 ,84 ,67 ,246 ,115 ,243 ,127 ,218 ,146 ,61 ,82 ,227 ,24 ,25 ,64 ,144 ,131 ,53 ,116 ,145 ,157 ,227}
	trie := newPresentp(root)
	f,_:=os.Open("E:/record/address.txt")
	s := bufio.NewScanner(f)
	index:=0
	var Address [1000100][]byte
	var shijian float64
	T1:=time.Now()
	for s.Scan() {
		str:=s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Address[index] = key
		index++
	}
	T2:=time.Now()
	rand.Seed(0)
	for i:=0;i<10000;i++{
		j:=rand.Intn(1000000)
		k:=Address[j]
		t1:=time.Now()
		trie.Get(crypto.Keccak256(k))
		t2:=time.Now()
		shijian += t2.Sub(t1).Seconds()
	}
	index = int(trie.PtintItems())
	fmt.Println("总共花费时间：",T2.Sub(T1).Seconds())
	fmt.Println("Get耗费时间：",shijian)
	fmt.Println(index)
	fmt.Println("qps:",float64(10000)/shijian)
	runtime.GC()
}

func TestMyGetP2(t *testing.T){
	// 未加前缀
	fmt.Println("1")
	root:=[]byte{240 ,253 ,95 ,176 ,27 ,69 ,145 ,183 ,146 ,0 ,44 ,84 ,14 ,103 ,158 ,113 ,179 ,234 ,103 ,238 ,213 ,41 ,139 ,151 ,162 ,135 ,10 ,193 ,131 ,237 ,51 ,111 ,35}
	trie := newPresentp2(root)
	f,_:=os.Open("E:/record/address.txt")
	s := bufio.NewScanner(f)
	index:=0
	var Address [1000100][]byte
	var shijian float64
	T1:=time.Now()
	for s.Scan() {
		str:=s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Address[index] = key
		index++
	}
	T2:=time.Now()
	rand.Seed(0)
	for i:=0;i<10000;i++{
		j:=rand.Intn(1000000)
		k:=Address[j]
		t1:=time.Now()
		trie.Get(crypto.Keccak256(k))
		t2:=time.Now()
		shijian += t2.Sub(t1).Seconds()
	}
	index = int(trie.PtintItems())
	fmt.Println("总共花费时间：",T2.Sub(T1).Seconds())
	fmt.Println("Get耗费时间：",shijian)
	fmt.Println(index)
	fmt.Println("qps:",float64(10000)/shijian)
	runtime.GC()
}

func TestInsert(t *testing.T) {
	trie := newEmpty() //new一棵树，用的内存数据库

	updateString(trie, "doe", "reindeer")//insert
	updateString(trie, "dog", "puppy")//update？
	updateString(trie, "dogglesworth", "cat")//insert

	exp := common.HexToHash("8aad789dff2f538bca5d8ea56e8abe10f4c7ba3a5dea95fea4cd6e7c3a1168d3")
	root := trie.Hash()
	if root != exp {
		t.Errorf("exp %x got %x", exp, root)
	}

	trie = newEmpty()
	updateString(trie, "A", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	str:="d23786fb4a010da3ce639d66d5e904a11dbc02746d1ce25029e53290cabf28ab"
	fmt.Println(str)
	//HexToHash,将hex编码string转化为Hash32字节
	exp = common.HexToHash("d23786fb4a010da3ce639d66d5e904a11dbc02746d1ce25029e53290cabf28ab")
	fmt.Println(exp)
	root,_, err := trie.Commit()
	fmt.Println(root)
	if err != nil {
		t.Fatalf("commit error: %v", err)
	}
	if root != exp {
		t.Errorf("exp %x got %x", exp, root)
	}
}

func TestGet(t *testing.T) {
	trie := newEmpty()
	updateString(trie, "doe", "reindeer")
	updateString(trie, "dog", "puppy")
	updateString(trie, "dogglesworth", "cat")

	for i := 0; i < 2; i++ {
		res := getString(trie, "dog")
		fmt.Println(res)
		if !bytes.Equal(res, []byte("puppy")) {
			t.Errorf("expected puppy got %x", res)
		}

		unknown := getString(trie, "unknown")
		if unknown != nil {
			t.Errorf("expected nil got %x", unknown)
		}

		if i == 1 {
			return
		}
		trie.Commit()
	}
}

func TestDelete(t *testing.T) {
	trie := newEmpty()
	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"ether", ""},
		{"dog", "puppy"},
		{"shaman", ""},
	}
	for _, val := range vals {
		if val.v != "" {
			updateString(trie, val.k, val.v)
		} else {
			deleteString(trie, val.k)
		}
	}

	hash := trie.Hash()
	exp := common.HexToHash("5991bb8c6514148a29db676a14ac506cd2cd5775ace63c30a4fe457715e9ac84")
	if hash != exp {
		t.Errorf("expected %x got %x", exp, hash)
	}
}

func TestEmptyValues(t *testing.T) {
	trie := newEmpty()

	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"ether", ""},
		{"dog", "puppy"},
		{"shaman", ""},
	}
	for _, val := range vals {
		updateString(trie, val.k, val.v)
	}

	hash := trie.Hash()
	exp := common.HexToHash("5991bb8c6514148a29db676a14ac506cd2cd5775ace63c30a4fe457715e9ac84")
	if hash != exp {
		t.Errorf("expected %x got %x", exp, hash)
	}
}

func TestReplication(t *testing.T) {
	trie := newEmpty()
	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"dog", "puppy"},
		{"somethingveryoddindeedthis is", "myothernodedata"},
	}
	for _, val := range vals {
		updateString(trie, val.k, val.v)
	}
	exp,_, err := trie.Commit()
	if err != nil {
		t.Fatalf("commit error: %v", err)
	}

	// create a new trie on top of the database and check that lookups work.
	trie2, err := New(exp, trie.db)
	if err != nil {
		t.Fatalf("can't recreate trie at %x: %v", exp, err)
	}
	for _, kv := range vals {
		if string(getString(trie2, kv.k)) != kv.v {
			t.Errorf("trie2 doesn't have %q => %q", kv.k, kv.v)
		}
	}
	hash,_, err := trie2.Commit()
	if err != nil {
		t.Fatalf("commit error: %v", err)
	}
	if hash != exp {
		t.Errorf("root failure. expected %x got %x", exp, hash)
	}

	// perform some insertions on the new trie.
	vals2 := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		// {"shaman", "horse"},
		// {"doge", "coin"},
		// {"ether", ""},
		// {"dog", "puppy"},
		// {"somethingveryoddindeedthis is", "myothernodedata"},
		// {"shaman", ""},
	}
	for _, val := range vals2 {
		updateString(trie2, val.k, val.v)
	}
	if hash := trie2.Hash(); hash != exp {
		t.Errorf("root failure. expected %x got %x", exp, hash)
	}
}

func TestLargeValue(t *testing.T) {
	trie := newEmpty()
	trie.Update([]byte("key1"), []byte{99, 99, 99, 99})
	trie.Update([]byte("key2"), bytes.Repeat([]byte{1}, 32))
	trie.Hash()
}

type countingDB struct {
	Database
	gets map[string]int
}

func (db *countingDB) Get(key []byte) ([]byte, error) {
	db.gets[string(key)]++
	return db.Database.Get(key)
}

// TestCacheUnload checks that decoded nodes are unloaded after a
// certain number of commit operations.
func TestCacheUnload(t *testing.T) {
	// Create test trie with two branches.
	trie := newEmpty()
	key1 := "---------------------------------"
	key2 := "---some other branch"
	updateString(trie, key1, "this is the branch of key1.")
	updateString(trie, key2, "this is the branch of key2.")
	root,_, _ := trie.Commit()

	// Commit the trie repeatedly and access key1.
	// The branch containing it is loaded from DB exactly two times:
	// in the 0th and 6th iteration.
	db := &countingDB{Database: trie.db, gets: make(map[string]int)}
	trie, _ = New(root, db)
	trie.SetCacheLimit(5)
	for i := 0; i < 12; i++ {
		getString(trie, key1)
		trie.Commit()
	}

	// Check that it got loaded two times.
	for dbkey, count := range db.gets {
		if count != 2 {
			t.Errorf("db key %x loaded %d times, want %d times", []byte(dbkey), count, 2)
		}
	}
}

// randTest performs random trie operations.
// Instances of this test are created by Generate.
type randTest []randTestStep

type randTestStep struct {
	op    int
	key   []byte // for opUpdate, opDelete, opGet
	value []byte // for opUpdate
	err   error  // for debugging
}

const (
	opUpdate = iota
	opDelete
	opGet
	opCommit
	opHash
	opReset
	opItercheckhash
	opCheckCacheInvariant
	opMax // boundary value, not an actual op
)

func (randTest) Generate(r *rand.Rand, size int) reflect.Value {
	var allKeys [][]byte
	genKey := func() []byte {
		if len(allKeys) < 2 || r.Intn(100) < 10 {
			// new key
			key := make([]byte, r.Intn(50))
			r.Read(key)
			allKeys = append(allKeys, key)
			return key
		}
		// use existing key
		return allKeys[r.Intn(len(allKeys))]
	}

	var steps randTest
	for i := 0; i < size; i++ {
		step := randTestStep{op: r.Intn(opMax)}
		switch step.op {
		case opUpdate:
			step.key = genKey()
			step.value = make([]byte, 8)
			binary.BigEndian.PutUint64(step.value, uint64(i))
		case opGet, opDelete:
			step.key = genKey()
		}
		steps = append(steps, step)
	}
	return reflect.ValueOf(steps)
}

//func runRandTest(rt randTest) bool {
//	db, _ := ethdb.NewMemDatabase()
//	tr, _ := New(common.Hash{}, db)
//	values := make(map[string]string) // tracks content of the trie
//
//	for i, step := range rt {
//		switch step.op {
//		case opUpdate:
//			tr.Update(step.key, step.value)
//			values[string(step.key)] = string(step.value)
//		case opDelete:
//			tr.Delete(step.key)
//			delete(values, string(step.key))
//		case opGet:
//			v := tr.Get(step.key)
//			want := values[string(step.key)]
//			if string(v) != want {
//				rt[i].err = fmt.Errorf("mismatch for key 0x%x, got 0x%x want 0x%x", step.key, v, want)
//			}
//		case opCommit:
//			_,_, rt[i].err = tr.Commit()
//		case opHash:
//			tr.Hash()
//		case opReset:
//			hash,_, err := tr.Commit()
//			if err != nil {
//				rt[i].err = err
//				return false
//			}
//			newtr, err := New(hash, db)
//			if err != nil {
//				rt[i].err = err
//				return false
//			}
//			tr = newtr
//		case opItercheckhash:
//			checktr, _ := New(common.Hash{}, nil)
//			it := NewIterator(tr.NodeIterator(nil))
//			for it.Next() {
//				checktr.Update(it.Key, it.Value)
//			}
//			if tr.Hash() != checktr.Hash() {
//				rt[i].err = fmt.Errorf("hash mismatch in opItercheckhash")
//			}
//		case opCheckCacheInvariant:
//			rt[i].err = checkCacheInvariant(tr.root, nil, tr.cachegen, false, 0)
//		}
//		// Abort the test on error.
//		if rt[i].err != nil {
//			return false
//		}
//	}
//	return true
//}

func checkCacheInvariant(n, parent node, parentCachegen uint16, parentDirty bool, depth int) error {
	var children []node
	var flag nodeFlag
	switch n := n.(type) {
	case *shortNode:
		flag = n.flags
		children = []node{n.Val}
	case *fullNode:
		flag = n.flags
		children = n.Children[:]
	default:
		return nil
	}

	errorf := func(format string, args ...interface{}) error {
		msg := fmt.Sprintf(format, args...)
		msg += fmt.Sprintf("\nat depth %d node %s", depth, spew.Sdump(n))
		msg += fmt.Sprintf("parent: %s", spew.Sdump(parent))
		return errors.New(msg)
	}
	if flag.gen > parentCachegen {
		return errorf("cache invariant violation: %d > %d\n", flag.gen, parentCachegen)
	}
	if depth > 0 && !parentDirty && flag.dirty {
		return errorf("cache invariant violation: %d > %d\n", flag.gen, parentCachegen)
	}
	for _, child := range children {
		if err := checkCacheInvariant(child, n, flag.gen, flag.dirty, depth+1); err != nil {
			return err
		}
	}
	return nil
}

//func TestRandom(t *testing.T) {
//	if err := quick.Check(runRandTest, nil); err != nil {
//		if cerr, ok := err.(*quick.CheckError); ok {
//			t.Fatalf("random test iteration %d failed: %s", cerr.Count, spew.Sdump(cerr.In))
//		}
//		t.Fatal(err)
//	}
//}

func BenchmarkGet(b *testing.B)      { benchGet(b, false) }

func BenchmarkGetDB(b *testing.B)    { benchGet(b, true) }

func BenchmarkUpdateBE(b *testing.B) { benchUpdate(b, binary.BigEndian) }

func BenchmarkUpdateLE(b *testing.B) { benchUpdate(b, binary.LittleEndian) }

const benchElemCount = 20000

func benchGet(b *testing.B, commit bool) {
	trie := new(Trie)
	if commit {
		_, tmpdb := tempDB()
		trie, _ = New(common.Hash{}, tmpdb)
	}
	k := make([]byte, 32)
	for i := 0; i < benchElemCount; i++ {
		binary.LittleEndian.PutUint64(k, uint64(i))
		trie.Update(k, k)
	}
	binary.LittleEndian.PutUint64(k, benchElemCount/2)
	if commit {
		trie.Commit()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		trie.Get(k)
	}
	b.StopTimer()

	if commit {
		ldb := trie.db.(*ethdb.LDBDatabase)
		ldb.Close()
		os.RemoveAll(ldb.Path())
	}
}

func benchUpdate(b *testing.B, e binary.ByteOrder) *Trie {
	trie := newEmpty()
	k := make([]byte, 32)
	for i := 0; i < b.N; i++ {
		e.PutUint64(k, uint64(i))
		trie.Update(k, k)
	}
	return trie
}

// Benchmarks the trie hashing. Since the trie caches the result of any operation,
// we cannot use b.N as the number of hashing rouns, since all rounds apart from
// the first one will be NOOP. As such, we'll use b.N as the number of account to
// insert into the trie before measuring the hashing.
func BenchmarkHash(b *testing.B) {
	// Make the random benchmark deterministic
	random := rand.New(rand.NewSource(0))

	// Create a realistic account trie to hash,[二维数组长度为b.N]
	addresses := make([][20]byte, b.N)
	for i := 0; i < len(addresses); i++ {
		for j := 0; j < len(addresses[i]); j++ {
			// b.N * 20
			addresses[i][j] = byte(random.Intn(256))
		}
	}
	// 长度和addresser中的个数一致，为b.N
	accounts := make([][]byte, len(addresses))
	for i := 0; i < len(accounts); i++ {
		var (
			nonce   = uint64(random.Int63())
			balance = new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
			root    = emptyRoot
			code    = crypto.Keccak256(nil)
		)
		// 为account赋值，分别为nonce、balance、root、code
		accounts[i], _ = rlp.EncodeToBytes([]interface{}{nonce, balance, root, code})
	}
	// Insert the accounts into the trie and hash it
	trie := newEmpty()
	for i := 0; i < len(addresses); i++ {
		//插入树，看执行效率
		trie.Update(crypto.Keccak256(addresses[i][:]), accounts[i])
	}
	b.ResetTimer()
	b.ReportAllocs()
	// node - hashNode
	trie.Hash() // Hash --> hashroot --> hash()
}

func BenchmarkHashMY(b *testing.B) {
	// Make the random benchmark deterministic
	random := rand.New(rand.NewSource(0))

	// Create a realistic account trie to hash,[二维数组长度为b.N]
	addresses := make([][20]byte, b.N)
	for i := 0; i < len(addresses); i++ {
		for j := 0; j < len(addresses[i]); j++ {
			// b.N * 20
			addresses[i][j] = byte(random.Intn(256))
		}
	}
	// 长度和addresser中的个数一致，为b.N
	accounts := make([][]byte, len(addresses))
	for i := 0; i < len(accounts); i++ {
		var (
			nonce   = uint64(random.Int63())
			balance = new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
			root    = emptyRoot
			code    = crypto.Keccak256(nil)
		)
		// 为account赋值，分别为nonce、balance、root、code
		accounts[i], _ = rlp.EncodeToBytes([]interface{}{nonce, balance, root, code})
	}
	// Insert the accounts into the trie and hash it
	trie := newEmpty()
	for i := 0; i < len(addresses); i++ {
		//插入树，看执行效率
		trie.Update(crypto.Keccak256(addresses[i][:]), accounts[i])
	}
	b.ResetTimer()
	b.ReportAllocs()
	// node - hashNode
	trie.Hash() // Hash --> hashroot --> hash()
}

func tempDB() (string, Database) {
	dir, err := ioutil.TempDir("", "trie-bench")
	if err != nil {
		panic(fmt.Sprintf("can't create temporary directory: %v", err))
	}
	db, err := ethdb.NewLDBDatabase(dir, 256, 0)
	if err != nil {
		panic(fmt.Sprintf("can't create temporary database: %v", err))
	}
	return dir, db
}

func getString(trie *Trie, k string) []byte {
	return trie.Get([]byte(k))
}

func updateString(trie *Trie, k, v string) {
	trie.Update([]byte(k), []byte(v))
}

func deleteString(trie *Trie, k string) {
	trie.Delete([]byte(k))
}
