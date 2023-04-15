package trie

import (
	"Exper"
	"MPT/ethdb"
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/opt"
	_ "github.com/syndtr/goleveldb/leveldb/opt"
	_ "io"
	log2 "log"
	"math/big"
	"math/rand"
	_ "math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

var (
	// databaseVerisionKey tracks the current database version.
	databaseVerisionKey = []byte("DatabaseVersion")

	// headHeaderKey tracks the latest known header's hash.
	headHeaderKey = []byte("LastHeader")

	// headBlockKey tracks the latest known full block's hash.
	headBlockKey = []byte("LastBlock")

	// headFastBlockKey tracks the latest known incomplete block's hash during fast sync.
	headFastBlockKey = []byte("LastFast")

	// fastTrieProgressKey tracks the number of trie entries imported during fast sync.
	fastTrieProgressKey = []byte("TrieSync")

	// snapshotRootKey tracks the hash of the last snapshot.
	snapshotRootKey = []byte("SnapshotRoot")

	// snapshotJournalKey tracks the in-memory diff layers across restarts.
	snapshotJournalKey = []byte("SnapshotJournal")

	// txIndexTailKey tracks the oldest block whose transactions have been indexed.
	txIndexTailKey = []byte("TransactionIndexTail")

	// fastTxLookupLimitKey tracks the transaction lookup limit during fast sync.
	fastTxLookupLimitKey = []byte("FastTransactionLookupLimit")

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	headerPrefix       = []byte("h") // headerPrefix + num (uint64 big endian) + hash -> header
	headerTDSuffix     = []byte("t") // headerPrefix + num (uint64 big endian) + hash + headerTDSuffix -> td
	headerHashSuffix   = []byte("n") // headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	headerNumberPrefix = []byte("H") // headerNumberPrefix + hash -> num (uint64 big endian)

	blockBodyPrefix     = []byte("b") // blockBodyPrefix + num (uint64 big endian) + hash -> block body
	blockReceiptsPrefix = []byte("r") // blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts

	txLookupPrefix        = []byte("l") // txLookupPrefix + hash -> transaction/receipt lookup metadata
	bloomBitsPrefix       = []byte("B") // bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash -> bloom bits
	SnapshotAccountPrefix = []byte("a") // SnapshotAccountPrefix + account hash -> account trie value
	SnapshotStoragePrefix = []byte("o") // SnapshotStoragePrefix + account hash + storage hash -> storage trie value

	preimagePrefix = []byte("secure-key-")      // preimagePrefix + hash -> preimage
	configPrefix   = []byte("ethereum-config-") // config prefix for the db

	// Chain index prefixes (use `i` + single byte to avoid mixing data types).
	BloomBitsIndexPrefix = []byte("iB") // BloomBitsIndexPrefix is the data table of a chain indexer to track its progress

	Count int
	tt1   float64
	tt2   float64
	tt3   float64
	tx    []byte
	ac    []byte
)

type LegacyTxLookupEntry struct {
	BlockHash  common.Hash
	BlockIndex uint64
	Index      uint64
}

func BytesToHash(b []byte) common.Hash {
	var h common.Hash
	h.SetBytes(b)
	return h
}
func headerNumberKey(hash common.Hash) []byte {
	return append(headerNumberPrefix, hash.Bytes()...)
}
func ReadHeaderNumber(db ethdb.LDBDatabase, hash common.Hash) uint64 {
	data, _ := db.Get(headerNumberKey(hash))
	if len(data) != 8 {
		return 0
	}
	number := binary.BigEndian.Uint64(data)
	return number
}
func GetTxLookupEntry(db ethdb.LDBDatabase, hash common.Hash) uint64 {
	// Load the positional metadata from disk and bail if it fails
	data, _ := db.Get(append(txLookupPrefix, hash.Bytes()...))
	if len(data) == 0 {
		return 0
	}
	if len(data) < common.HashLength {
		number := new(big.Int).SetBytes(data).Uint64()
		return number
	}
	if len(data) == common.HashLength {
		return ReadHeaderNumber(db, common.BytesToHash(data))
	}
	// Finally try database v3 tx lookup format
	var entry LegacyTxLookupEntry
	if err := rlp.DecodeBytes(data, &entry); err != nil {
		log.Error("Invalid transaction lookup entry RLP", "hash", hash, "blob", data, "err", err)
		return 0
	}
	return 0
}
func GetTxLookupEntry_s(db ethdb.LDBDatabase, hash common.Hash) uint64 {
	// Load the positional metadata from disk and bail if it fails
	data, _ := db.Get_s(append(txLookupPrefix, hash.Bytes()...))
	if len(data) == 0 {
		return 0
	}
	if len(data) < common.HashLength {
		number := new(big.Int).SetBytes(data).Uint64()
		return number
	}
	if len(data) == common.HashLength {
		return ReadHeaderNumber(db, common.BytesToHash(data))
	}
	// Finally try database v3 tx lookup format
	var entry LegacyTxLookupEntry
	if err := rlp.DecodeBytes(data, &entry); err != nil {
		log.Error("Invalid transaction lookup entry RLP", "hash", hash, "blob", data, "err", err)
		return 0
	}
	return 0
}
func headerKey(hash common.Hash, number uint64) []byte {
	return append(append(headerPrefix, encodeBlockNumber(number)...), hash.Bytes()...)
}
func hashKey(number uint64) []byte {
	return append(append(headerPrefix, encodeBlockNumber(number)...), headerHashSuffix...)
}
func encodeBlockNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}
func blockBodyKey(hash common.Hash, number uint64) []byte {
	return append(append(blockBodyPrefix, encodeBlockNumber(number)...), hash.Bytes()...)
}
func GetBodyRLP(db ethdb.LDBDatabase, hash common.Hash, number uint64) rlp.RawValue {
	data, _ := db.Get(blockBodyKey(hash, number))
	return data
}
func GetBodyRLP_s(db ethdb.LDBDatabase, hash common.Hash, number uint64) rlp.RawValue {
	data, _ := db.Get_s(blockBodyKey(hash, number))
	return data
}
func GetBody(db ethdb.LDBDatabase, hash common.Hash, number uint64) *types.Body {
	data := GetBodyRLP(db, hash, number)
	//fmt.Println("Body",data)
	if len(data) == 0 {
		return nil
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil
	}
	return body
}
func GetBody_s(db ethdb.LDBDatabase, hash common.Hash, number uint64) *types.Body {
	data := GetBodyRLP_s(db, hash, number)
	//fmt.Println("Body",data)
	if len(data) == 0 {
		return nil
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil
	}
	return body
}
func GetTransaction(db ethdb.LDBDatabase, hash common.Hash) (*types.Transaction, common.Hash, uint64, uint64) {
	// Retrieve the lookup metadata and resolve the transaction from the body
	// 取出区块号
	t1 := time.Now()
	blockNumber := GetTxLookupEntry(db, hash)
	t2 := time.Now()
	tt1 += t2.Sub(t1).Seconds()
	//fmt.Println("blocknumber:",blockNumber)
	headerhash := hashKey(blockNumber) // prefix + num + suffix --> hash
	// 取区块hash
	t3 := time.Now()
	blkhash3, _ := db.Get(headerhash)
	t4 := time.Now()
	tt2 += t4.Sub(t3).Seconds()
	body := GetBody(db, BytesToHash(blkhash3), blockNumber) // b + num + hash --> body
	t5 := time.Now()
	tt3 += t5.Sub(t4).Seconds()
	if body == nil {
		Count++
	}
	//fmt.Println(blkhash3)
	//body := GetBody(db,BytesToHash(blkhash3),blockNumber) // b + num + hash --> body
	//fmt.Println("Type.Body",body)
	//if body == nil {
	//	log.Error("Transaction referenced missing", "number", blockNumber, "hash", blkhash3)
	//	return nil, common.Hash{}, 0, 0
	//}
	//for txIndex, tx := range body.Transactions {
	//	if tx.Hash() == hash {
	//		return tx, BytesToHash(blkhash3), blockNumber, uint64(txIndex)
	//	}
	//}
	//log.Error("Transaction not found", "number", blockNumber, "hash", blkhash3, "txhash", hash)
	return nil, common.Hash{}, 0, 0
}
func GetTransaction_s(db ethdb.LDBDatabase, hash common.Hash) (*types.Transaction, common.Hash, uint64, uint64) {
	t1 := time.Now()
	blockNumber := GetTxLookupEntry_s(db, hash)
	t2 := time.Now()
	tt1 += t2.Sub(t1).Seconds()
	//fmt.Println("blocknumber:",blockNumber)
	headerhash := hashKey(blockNumber) // prefix + num + suffix --> hash
	// 取区块hash
	t3 := time.Now()
	blkhash3, _ := db.Get_s(headerhash)
	t4 := time.Now()
	tt2 += t4.Sub(t3).Seconds()
	body := GetBody_s(db, BytesToHash(blkhash3), blockNumber) // b + num + hash --> body
	t5 := time.Now()
	tt3 += t5.Sub(t4).Seconds()
	if body == nil {
		Count++
	}
	//fmt.Println("Type.Body",body)
	//if body == nil {
	//	log.Error("Transaction referenced missing", "number", blockNumber, "hash", blkhash3)
	//	return nil, common.Hash{}, 0, 0
	//}
	//for txIndex, tx := range body.Transactions {
	//	//	if tx.Hash() == hash {
	//	//		return tx, BytesToHash(blkhash3), blockNumber, uint64(txIndex)
	//	//	}
	//	//}
	//	//log.Error("Transaction not found", "number", blockNumber, "hash", blkhash3, "txhash", hash)
	return nil, common.Hash{}, 0, 0
}

var (
	Account [11000000][]byte
	Txhash  [11000000][]byte
	TXhash  [30000010][]byte
	root1  = []byte{132, 0, 59, 165, 252, 52, 160, 208, 92, 17, 70, 162, 180, 18, 77, 149, 155, 195, 222, 231, 14, 5, 150, 170, 177, 231, 181, 6, 178, 219, 70, 170}
	root2  = []byte{240, 114, 194, 194, 17, 14, 199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	root3  = []byte{16, 154, 137, 29, 83, 174, 214, 190, 39, 12, 125, 65, 0, 137, 9, 204, 24, 237, 131, 58, 150, 132, 67, 65, 170, 245, 4, 35, 252, 67, 249, 33, 134}
	//root2 = []byte{240 ,215, 251, 217, 93, 11, 72, 181, 8, 163, 61 ,123, 113, 247, 127, 82, 177, 39, 173, 39, 27, 77, 242, 166, 83, 234, 9, 77, 143, 76, 232, 97, 250}
)

func TestMix(t *testing.T) {
	//root:=[]byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	root := []byte{240, 114, 194, 194, 17, 14, 199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	db, _ := ethdb.NewLDBDatabase("C:/DB/Double-P", 16, 128)
	//tree, _ := News(root, db)
	f, _ := os.Open("F:/paper/record/UniqueA2.txt")
	s := bufio.NewScanner(f)
	Count_T := 0
	Count_A := 0
	var shijian float64
	for s.Scan() {

		str := s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Account[Count_A] = key
		Count_A++
		if Count_A == 10100000 {
			break
		}
	}
	_ = f.Close()

	f1, _ := os.Open("F:/paper/record/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0
	for s1.Scan() {
		str := s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber%8 == 0 {
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10100000 {
			break
		}
	}
	_ = f1.Close()
	number := 0
	Count2 := 0
	t1 := time.Now()
	f2, _ := os.Open("F:/paper/record/Merge.txt")
	s2 := bufio.NewScanner(f2)
	var (
		num [1000001]int
		xx  int
		yy  int
	)
	index := 0

	for s2.Scan() {
		str := s2.Text()
		sort, _ := strconv.Atoi(str)
		num[index] = sort
		index++
	}
	for i := 1; i < 1000001; i++ {
		for j := 1; j < 2; j++ {
			tx = Txhash[num[xx]]
			txh := BytesToHash(tx)
			GetTransaction_s(*db, txh)
			xx++
		}
		for k := 1; k < 2; k++ {
			ac = Account[num[yy]]
			tree, _ := News(root, db)
			v := tree.Get(crypto.Keccak256(ac))
			if v == nil {
				Count++
			}
			yy++
		}
		number++
		if number == 1000000 {
			break
		}
	}
	_ = f2.Close()
	t2 := time.Now()
	shijian += t2.Sub(t1).Seconds()
	TimeTx := tt1 + tt2 + tt3
	fmt.Println("总时间耗费：", shijian)
	fmt.Println("nil计数", Count, Count2)
	fmt.Println("kv数目,总时间，交易时间", ethdb.Count, ethdb.T, TimeTx, shijian)
	fmt.Println("qps:", float64(1000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	runtime.GC()
}

func TestMixRandom(t *testing.T) {
	//rroot:=[]byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14, 199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	idle0, total0 := Exper.GetCPUSample()
	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Double-P", 16, 128)
	//db,_ := ethdb.NewLDBDatabase2("C:/DB/State",16,512)
	//db2,_ := ethdb.NewLDBDatabase2("C:/DB/Tx",16,512)
	//tree, _ := News(root, db)
	f, _ := os.Open("/media/czh/sn/record/UniqueA2.txt")
	s := bufio.NewScanner(f)
	Count_T := 0
	Count_A := 0
	var shijian float64
	for s.Scan() {
		str := s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Account[Count_A] = key
		Count_A++
		if Count_A == 10100000 {
			break
		}
	}
	_ = f.Close()

	f1, _ := os.Open("/media/czh/sn/record/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0
	for s1.Scan() {
		str := s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber%8 == 0 {
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10100000 {
			break
		}
	}
	_ = f1.Close()
	fmt.Println("==============")
	number := 0
	Count2 := 0
	t1 := time.Now()
	rand.Seed(0)
	//num := 1000000
	var old_hit  int64
	var old_miss  int64
	var old_hit2  int64
	var old_miss2  int64
	init_time := time.Now()
	for q:=0;q<=10;q++{
		for i := 0; i < 100000; i++ {
			for m:=0;m<(10-q);m++{
				j := rand.Intn(10000000)
				tx = Txhash[j]
				txh := BytesToHash(tx)
				GetTransaction_s(*db, txh)
				number++
			}
			for n:=0;n<q;n++{
				j := rand.Intn(10000000)
				ac = Account[j]
				tree, _ := News(root2, db)
				v2 := tree.Get(crypto.Keccak256(ac))
				if v2 == nil {
					Count2++
				}
				number++
			}
			//if i%100000 == 0 {
			//	log2.Println(i)
			//}
			if number % 100000 == 0{
				hit:=cache.HitNumber
				miss:=cache.MissNumber
				hit2:=cache.HitNumber2
				miss2:=cache.MissNumber2
				log2.Println("The hit rate of",(number/100000),"is:",float64(hit+hit2-old_hit-old_hit2)/float64(hit+miss+hit2+miss2-old_hit-old_miss-old_hit2-old_miss2))
				old_miss = miss
				old_hit = hit
				old_miss2 = miss2
				old_hit2 = hit2

				ttt:=time.Now()
				tttt:=ttt.Sub(init_time).Seconds()
				fmt.Println("The QPS is:",float64(100000)/tttt)
				init_time = ttt
			}
		}
	}
	t2 := time.Now()
	shijian += t2.Sub(t1).Seconds()
	TimeTx := tt1 + tt2 + tt3
	fmt.Println("Get耗费时间：", shijian)
	fmt.Println(Count, Count2)
	fmt.Println(ethdb.Count, ethdb.T, TimeTx, shijian)
	fmt.Println("qps:", float64(9000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	idle1, total1 := Exper.GetCPUSample()
	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks
	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)
	runtime.GC()
}
// Test the Tx:Acc Ratio is 0:1 in Sc and Sk Distribution
func TestAccZ(t *testing.T) {
	//root:=[]byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14, 199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Original", 16, 128)
	//tree, _ := News(root, db)
	f, _ := os.Open("/media/czh/sn/record/UniqueA2.txt")
	s := bufio.NewScanner(f)
	Count_A := 0
	for s.Scan() {
		str := s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Account[Count_A] = key
		Count_A++
		if Count_A == 10100000 {
			break
		}
	}
	_ = f.Close()
	number := 0
	//Count2 := 0
	//var NUM[10000001]int
	f2, _ := os.Open("/media/czh/sn/record/Scrambled.txt")
	s2 := bufio.NewScanner(f2)
	for s2.Scan() {
		str := s2.Text()
		j, _ := strconv.Atoi(str)
		tree, _ := News(root3, db)
		ac := Account[j]
		v := tree.Get(crypto.Keccak256(ac))
		if v == nil {
			Count++
		}
		number++
		if number%100000 == 0 {
			log2.Println(number)
		}
	}
	_ = f2.Close()
	TimeTx := tt1 + tt2 + tt3
	fmt.Println("nil计数", Count, number)
	fmt.Println("kv数目,总时间，交易时间", ethdb.Count, ethdb.T, TimeTx)
	fmt.Println("qps:", float64(10000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	runtime.GC()
}
// Test the Tx:Acc Ratio is 0:1 in Normal Random Distribution
func TestAccR(t *testing.T) {
	idle0, total0 := Exper.GetCPUSample()
	//root:=[]byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14, 199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}

	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Original", 16, 128)
	//tree, _ := News(root, db)
	f, _ := os.Open("/media/czh/sn/record/UniqueA2.txt")
	s := bufio.NewScanner(f)
	Count_A := 0
	var shijian float64
	for s.Scan() {
		str := s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Account[Count_A] = key
		Count_A++
		if Count_A == 10100000 {
			break
		}
	}
	_ = f.Close()
	number := 0
	t1 := time.Now()
	rand.Seed(0)
	const num = 10000000
	for i := 0; i < num; i++ {
		j := rand.Intn(10000000)
		ac := Account[j]
		tree, _ := News(root3, db)
		v := tree.Get(crypto.Keccak256(ac))
		if v == nil {
			Count++
		}
		if i%100000 == 0 {
			log2.Println(i)
		}
		number++
	}
	t2 := time.Now()
	shijian += t2.Sub(t1).Seconds()
	TimeTx := tt1 + tt2 + tt3
	fmt.Println("总时间耗费：", shijian)
	fmt.Println("nil计数", Count, number)
	fmt.Println("kv数目,总时间，交易时间", ethdb.Count, ethdb.T, TimeTx, shijian)
	fmt.Println("qps:", float64(num)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	idle1, total1 := Exper.GetCPUSample()
	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks
	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)

	runtime.GC()
}
// Test the Tx:Acc Ratio is 1:0 in Normal Random Distribution
func TestTR(t *testing.T) {
	//rroot:=[]byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14, 199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	//132 0 59 165 252 52 160 208 92 17 70 162 180 18 77 149 155 195 222 231 14 5 150 170 177 231 181 6 178 219 70 170
	idle0, total0 := Exper.GetCPUSample()
	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Tx", 16, 128)
	f1, _ := os.Open("/media/czh/sn/record/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0
	Count_T := 0
	for s1.Scan() {
		str := s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber%8 == 0 {
			TXhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10000000 {
			break
		}
	}
	_ = f1.Close()
	fmt.Println("==============")
	number := 0
	Count2 := 0
	rand.Seed(0)
	for i := 0; i < 10000000; i++ {
		j := rand.Intn(10000000)
		tx = TXhash[j]
		txh := BytesToHash(tx)
		GetTransaction(*db, txh)
		number++
		if i%100000 == 0 {
			log2.Println(i)
		}
	}
	TimeTx := tt1 + tt2 + tt3
	fmt.Println(Count, Count2, Count_T)
	fmt.Println(ethdb.Count, ethdb.T, TimeTx)
	fmt.Println("qps:", float64(1000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	idle1, total1 := Exper.GetCPUSample()

	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks

	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)
	runtime.GC()
}
// Test the Tx:Acc Ratio is 1:0 in Sc and Sk Distribution
func TestTZ(t *testing.T) {
	//root:=[]byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14, 199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Single", 16, 128)
	//tree, _ := News(root, db)
	f1, _ := os.Open("/media/czh/sn/record/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0
	Count_T := 0
	for s1.Scan() {
		str := s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber%8 == 0 {
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10000000 {
			break
		}
	}
	_ = f1.Close()
	number := 0
	//Count2 := 0
	//var NUM[10000001]int
	f2, _ := os.Open("/media/czh/sn/record/Skewed.txt")
	s2 := bufio.NewScanner(f2)
	for s2.Scan() {
		str := s2.Text()
		j, _ := strconv.Atoi(str)
		GetTransaction(*db, BytesToHash(Txhash[j]))
		number++
		if number%100000 == 0 {
			log2.Println(number)
		}
	}
	_ = f2.Close()
	TimeTx := tt1 + tt2 + tt3
	fmt.Println("nil计数", Count, number)
	fmt.Println("kv数目,总时间，交易时间", ethdb.Count, ethdb.T, TimeTx)
	fmt.Println("qps:", float64(10000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	runtime.GC()
}

func TestRoot(t *testing.T) {
	db, _ := leveldb.OpenFile("/media/czh/sn/DB/Single", &opt.Options{
		Compression: opt.NoCompression,
		ReadOnly:    true,
	})
	for i := 3333333; i <= 4650000; i++ {
		headerhash := hashKey(uint64(i))
		fmt.Println("The Key of blkhash:", headerhash)
		blkhash, _ := db.Get(headerhash, nil)
		fmt.Println("Blkhash is:", blkhash)
		head := headerKey(BytesToHash(blkhash), uint64(i))
		blockHeaderData, _ := db.Get(head, nil)
		blockHeader := new(types.Header)
		tmpByteData := bytes.NewReader(blockHeaderData)
		rlp.Decode(tmpByteData, blockHeader)
		fmt.Println("State Root:", blockHeader.Root)
		fmt.Println("Receipt Root", blockHeader.ReceiptHash)
		fmt.Println("Tx Root:", blockHeader.TxHash)
		fmt.Println("The number of block:", blockHeader.Number)
		break
	}
}

func HashtoByte(hash common.Hash) []byte {
	h := hash[:]
	return h
}

// obtain all state root hash in 0~4.6 million blocks
func TestAllRoot(t *testing.T) {
	db, _ := leveldb.OpenFile("/media/czh/sn/DB/Single", &opt.Options{
		Compression: opt.NoCompression,
		ReadOnly:    true,
	})
	f, _ := os.OpenFile("/media/czh/sn/DB/state", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	var state, r, tx int
	for i := 0; i <= 4650000; i++ {
		headerhash := hashKey(uint64(i))
		//fmt.Println("The Key of blkhash:",headerhash)
		blkhash, _ := db.Get(headerhash, nil)
		//fmt.Println("Blkhash is:",blkhash)
		head := headerKey(BytesToHash(blkhash), uint64(i))
		blockHeaderData, _ := db.Get(head, nil)
		blockHeader := new(types.Header)
		tmpByteData := bytes.NewReader(blockHeaderData)
		_ = rlp.Decode(tmpByteData, blockHeader)
		stateroot, _ := db.Get(HashtoByte(blockHeader.Root), nil)
		receiptroot, _ := db.Get(HashtoByte(blockHeader.ReceiptHash), nil)
		txroot, _ := db.Get(HashtoByte(blockHeader.TxHash), nil)
		if stateroot != nil {
			state++
			log2.Println("StateRoot:", i)
			fmt.Fprintln(f, hex.EncodeToString(HashtoByte(blockHeader.Root)))
		}
		if receiptroot != nil {
			r++
			log2.Println("ReceiptRoot", i)
		}
		if txroot != nil {
			tx++
			log2.Println("TxRoot", i)
		}
		if i%2000000 == 0 {
			log2.Println("===================", i, "===================")
		}
	}
	fmt.Println(state, r, tx)
}

func TestFile(t *testing.T) {
	f, _ := os.Open("/media/czh/sn/DB/state")
	defer f.Close()

	index := 0
	index2 := 0
	s := bufio.NewScanner(f)
	for s.Scan() {
		str := s.Text()
		index++
		if len(str) == 64 {
			index2++
		}
	}
	fmt.Println(index, index2)
}

func TestFile2(t *testing.T) {
	idle0, total0 := Exper.GetCPUSample()
	//root:=[]byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14, 199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Single", 16, 1024)
	//tree, _ := News(root, db)
	f, _ := os.Open("/media/czh/sn/DB/state")
	defer f.Close()

	index := 0
	index2 := 0
	index3 := 0
	var root []byte
	s := bufio.NewScanner(f)
	for s.Scan() {
		str := s.Text()
		index++
		//max is 4565
		root, _ = hex.DecodeString(str)

	}
	fmt.Println(root)
	ff, _ := os.Open("/media/czh/sn/record/UniqueA2.txt")
	ss := bufio.NewScanner(ff)
	for ss.Scan() {
		str := ss.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		tree, _ := News(root, db)
		vv := tree.Get(crypto.Keccak256(key))
		if vv == nil {
			index3++
		}
		index2++
		if index2%100000 == 0 {
			log2.Println(index2)
		}
		if index2 == 1000000 {
			break
		}
	}
	fmt.Println("nil计数", index3)
	fmt.Println("kv数目,总时间,", ethdb.Count, ethdb.T)
	fmt.Println("qps:", float64(1000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	idle1, total1 := Exper.GetCPUSample()
	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks
	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)

	runtime.GC()
}

func TestFile3(t *testing.T) {
	//root:=[]byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14, 199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Single", 16, 1024)
	//tree, _ := News(root, db)
	f, _ := os.Open("/media/czh/sn/DB/state")
	defer f.Close()
	index := 0
	index2 := 0
	s := bufio.NewScanner(f)
	for s.Scan() {
		str := s.Text()
		index++
		k, _ := hex.DecodeString(str)
		v, _ := db.Get(k)
		if len(v) >= 512 {
			index2++
			fmt.Println(len(v))
		}

	}
	fmt.Println(index2)

}
// Test All workloads with Sc and Sk Distribution in the Tx:Acc Ratio of 1:1
func TestMixZipf(t *testing.T) {
	//	root := []byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14,
	//199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	//db,_ := ethdb.NewLDBDatabase("C:/DB/Double-P",16,1024)
	idle0, total0 := Exper.GetCPUSample()
	db, _ := ethdb.NewLDBDatabase2("/media/czh/sn/DB/Original", 16, 128)
	//	db2, _ := ethdb.NewLDBDatabase2("/media/czh/sn/DB/Tx", 16, 128)
	//tree, _ := News(root, db)
	f, _ := os.Open("/media/czh/sn/record/UniqueA2.txt")
	s := bufio.NewScanner(f)
	Count_T := 0
	Count_A := 0
	var shijian float64
	for s.Scan() {
		str := s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Account[Count_A] = key
		Count_A++
		if Count_A == 10100000 {
			break
		}
	}
	_ = f.Close()

	f1, _ := os.Open("/media/czh/sn/record/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0
	for s1.Scan() {
		str := s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber%8 == 0 {
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10100000 {
			break
		}
	}
	_ = f1.Close()
	number := 0
	Count2 := 0
	t1 := time.Now()
	f2, _ := os.Open("/media/czh/sn/record/Skewed.txt")
	s2 := bufio.NewScanner(f2)
	for s2.Scan() {
		str := s2.Text()
		sort, _ := strconv.Atoi(str)
		tx = Txhash[sort]
		txh := BytesToHash(tx)
		GetTransaction_s(*db, txh)
		ac = Account[sort]
		tree, _ := News(root3, db)
		v := tree.Get(crypto.Keccak256(ac))
		if v == nil {
			Count++
		}
		number++
		if number % 100000 == 0{
			log2.Println(number)
		}
	}
	_ = f2.Close()
	t2 := time.Now()
	shijian += t2.Sub(t1).Seconds()
	TimeTx := tt1 + tt2 + tt3
	fmt.Println("总时间耗费：", shijian)
	fmt.Println("nil计数", Count, Count2, number)
	fmt.Println("kv数目,总时间，交易时间", ethdb.Count, ethdb.T, TimeTx, shijian)
	fmt.Println("qps:", float64(10000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))

	idle1, total1 := Exper.GetCPUSample()
	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks
	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)

	runtime.GC()
}
// Test All workloads with Sc and Sk Distribution in the Tx:Acc Ratio of 3:7
func TestMixZ3(t *testing.T) {
	//	root := []byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14,
	//199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	//db,_ := ethdb.NewLDBDatabase("C:/DB/Double-P",16,1024)
	idle0, total0 := Exper.GetCPUSample()
	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Original", 16, 128)
//	db2, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Tx", 16, 128)
	//tree, _ := News(root, db)
	f, _ := os.Open("/media/czh/sn/record/UniqueA2.txt")
	s := bufio.NewScanner(f)
	Count_T := 0
	Count_A := 0
	var shijian float64
	for s.Scan() {
		str := s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Account[Count_A] = key
		Count_A++
		if Count_A == 10100000 {
			break
		}
	}
	_ = f.Close()

	f1, _ := os.Open("/media/czh/sn/record/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0
	for s1.Scan() {
		str := s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber%8 == 0 {
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10100000 {
			break
		}
	}
	_ = f1.Close()
	number := 0
	Count2 := 0
	t1 := time.Now()
	f3,_:=os.Open("/media/czh/sn/record/300.txt")
	defer f3.Close()
	s3:=bufio.NewScanner(f3)
	index3:=0
	var Array1[10000000]int
	for s3.Scan(){
		str:=s3.Text()
		sort, _ :=strconv.Atoi(str)
		Array1[index3] = sort
		index3++
	}
	f7,_:=os.Open("/media/czh/sn/record/700.txt")
	defer f7.Close()
	s7:=bufio.NewScanner(f7)
	index7:=0
	var Array2[10000000]int
	for s7.Scan(){
		str:=s7.Text()
		sort, _ :=strconv.Atoi(str)
		Array2[index7] = sort
		index7++
	}
	count3:=0
	count7:=0
	for i:=0;i<1000000;i++{
		 for m:=0;m<3;m++{
			 tx =  Txhash[Array1[count3]]
			 txh := BytesToHash(tx)
			 GetTransaction_s(*db, txh)
			 count3++
		 }
		 for n:=0;n<7;n++{
			 ac = Account[Array2[count7]]
			 tree, _ := News(root3, db)
			 v := tree.Get(crypto.Keccak256(ac))
			 if v == nil {
				 Count++
			 }
			 count7++
		 }
		 if i % 100000 == 0{
		 	log2.Println(i)
		 }
	}
	t2 := time.Now()
	shijian += t2.Sub(t1).Seconds()
	TimeTx := tt1 + tt2 + tt3
	fmt.Println("总时间耗费：", shijian)
	fmt.Println("nil计数", Count, Count2, number)
	fmt.Println("kv数目,总时间，交易时间", ethdb.Count, ethdb.T, TimeTx, shijian)
	fmt.Println("qps:", float64(10000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	fmt.Println(count3,count7)
	idle1, total1 := Exper.GetCPUSample()
	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks
	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)

	runtime.GC()
}
// Test All workloads with Sc and Sk Distribution in the Tx:Acc Ratio of 7:3
func TestMixZ7(t *testing.T) {
	//	root := []byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14,
	//199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	//db,_ := ethdb.NewLDBDatabase("C:/DB/Double-P",16,1024)
	idle0, total0 := Exper.GetCPUSample()
	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Original", 16, 128)
	//	db2, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Tx", 16, 128)
	//tree, _ := News(root, db)
	f, _ := os.Open("/media/czh/sn/record/UniqueA2.txt")
	s := bufio.NewScanner(f)
	Count_T := 0
	Count_A := 0
	var shijian float64
	for s.Scan() {
		str := s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Account[Count_A] = key
		Count_A++
		if Count_A == 10100000 {
			break
		}
	}
	_ = f.Close()

	f1, _ := os.Open("/media/czh/sn/record/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0
	for s1.Scan() {
		str := s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber%8 == 0 {
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10100000 {
			break
		}
	}
	_ = f1.Close()
	number := 0
	Count2 := 0
	t1 := time.Now()
	f3,_:=os.Open("/media/czh/sn/record/300.txt")
	defer f3.Close()
	s3:=bufio.NewScanner(f3)
	index3:=0
	var Array1[10000000]int
	for s3.Scan(){
		str:=s3.Text()
		sort, _ :=strconv.Atoi(str)
		Array1[index3] = sort
		index3++
	}
	f7,_:=os.Open("/media/czh/sn/record/700.txt")
	defer f7.Close()
	s7:=bufio.NewScanner(f7)
	index7:=0
	var Array2[10000000]int
	for s7.Scan(){
		str:=s7.Text()
		sort, _ :=strconv.Atoi(str)
		Array2[index7] = sort
		index7++
	}
	count3:=0
	count7:=0
	for i:=0;i<1000000;i++{
		for m:=0;m<7;m++{
			tx =  Txhash[Array2[count7]]
			txh := BytesToHash(tx)
			GetTransaction_s(*db, txh)
			count7++
		}
		for n:=0;n<3;n++{
			ac = Account[Array1[count3]]
			tree, _ := News(root3, db)
			v := tree.Get(crypto.Keccak256(ac))
			if v == nil {
				Count++
			}
			count3++
		}
		if i % 100000 == 0{
			log2.Println(i)
		}
	}
	t2 := time.Now()
	shijian += t2.Sub(t1).Seconds()
	TimeTx := tt1 + tt2 + tt3
	fmt.Println("总时间耗费：", shijian)
	fmt.Println("nil计数", Count, Count2, number)
	fmt.Println("kv数目,总时间，交易时间", ethdb.Count, ethdb.T, TimeTx, shijian)
	fmt.Println("qps:", float64(10000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	fmt.Println(count3,count7)
	idle1, total1 := Exper.GetCPUSample()
	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks
	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)

	runtime.GC()
}

func TestCacheZipfian(t *testing.T) {
	//	root := []byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14,
	//199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	//db,_ := ethdb.NewLDBDatabase("C:/DB/Double-P",16,1024)
	idle0, total0 := Exper.GetCPUSample()
	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Double-P", 16, 128)
	//tree, _ := News(root, db)
	f, _ := os.Open("/media/czh/sn/record/UniqueA2.txt")
	s := bufio.NewScanner(f)
	Count_T := 0
	Count_A := 0
	var shijian float64
	for s.Scan() {
		str := s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Account[Count_A] = key
		Count_A++
		if Count_A == 10100000 {
			break
		}
	}
	_ = f.Close()

	f1, _ := os.Open("/media/czh/sn/record/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0
	for s1.Scan() {
		str := s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber%8 == 0 {
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10100000 {
			break
		}
	}
	_ = f1.Close()
	number := 0
	Count2 := 0
	t1 := time.Now()
	f3,_:=os.Open("/media/czh/sn/record/Sk1_450.txt")
	defer f3.Close()
	s3:=bufio.NewScanner(f3)
	index3:=0
	var Array1[10000000]int
	for s3.Scan(){
		str:=s3.Text()
		sort, _ :=strconv.Atoi(str)
		Array1[index3] = sort
		index3++
	}
	f7,_:=os.Open("/media/czh/sn/record/Sk2_450.txt")
	defer f7.Close()
	s7:=bufio.NewScanner(f7)
	index7:=0
	var Array2[10000000]int
	for s7.Scan(){
		str:=s7.Text()
		sort, _ :=strconv.Atoi(str)
		Array2[index7] = sort
		index7++
	}
	count3:=0
	count7:=0
	var old_hit  int64
	var old_miss  int64
	var old_hit2  int64
	var old_miss2  int64
	var old_time float64
	init_time := time.Now()
	for q:=0;q<=10;q++{
		for i := 0; i < 100000; i++ {
			for m:=0;m<(10-q);m++{
				j := Array1[count3]
				tx = Txhash[j]
				txh := BytesToHash(tx)
				GetTransaction_s(*db, txh)
				count3++
				number++
			}
			for n:=0;n<q;n++{
				j := Array2[count7]
				ac = Account[j]
				tree, _ := News(root2, db)
				v2 := tree.Get(crypto.Keccak256(ac))
				if v2 == nil {
					Count2++   //记录nil
				}
				count7++
				number++
			}
			//if i%100000 == 0 {
			//	log2.Println(i)
			//}
			if number % 100000 == 0{
				hit:=cache.HitNumber
				miss:=cache.MissNumber
				hit2:=cache.HitNumber2
				miss2:=cache.MissNumber2
				log2.Println("The hit rate of",(number/100000),"is:",float64(hit+hit2-old_hit-old_hit2)/float64(hit+miss+hit2+miss2-old_hit-old_miss-old_hit2-old_miss2))
				old_miss = miss
				old_hit = hit
				old_miss2 = miss2
				old_hit2 = hit2

				ttt:=time.Now()
				tttt:=ttt.Sub(init_time).Seconds()
				fmt.Println("The QPS is:",float64(100000)/tttt,"The Real QPS is:",float64(100000)/(ethdb.T-old_time))
				old_time = ethdb.T
				init_time = ttt
			}
		}
	}
	//for i:=0;i<10000;i++{
	//	for m:=0;m<3;m++{
	//		tx =  Txhash[Array1[count3]]
	//		txh := BytesToHash(tx)
	//		GetTransaction(*db, txh)
	//		count3++
	//	}
	//	for n:=0;n<7;n++{
	//		ac = Account[Array2[count7]]
	//		tree, _ := News(root2, db)
	//		v := tree.Get(crypto.Keccak256(ac))
	//		if v == nil {
	//			Count++
	//		}
	//		count7++
	//	}
	//	if i % 100000 == 0{
	//		log2.Println(i)
	//	}
	//}
	t2 := time.Now()
	shijian += t2.Sub(t1).Seconds()
	TimeTx := tt1 + tt2 + tt3
	fmt.Println("总时间耗费：", shijian)
	fmt.Println("nil计数", Count, Count2, number)
	fmt.Println("kv数目,总时间，交易时间", ethdb.Count, ethdb.T, TimeTx, shijian)
	fmt.Println("qps:", float64(10000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	fmt.Println(count3,count7)
	idle1, total1 := Exper.GetCPUSample()
	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks
	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)

	runtime.GC()
}

// Test Tx:Acc Ratio is 1:1  with Normal Random Distribution
func TestMixR(t *testing.T) {
	//	root := []byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14,
	//199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	//db,_ := ethdb.NewLDBDatabase("C:/DB/Double-P",16,1024)
	idle0, total0 := Exper.GetCPUSample()
	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Original", 16, 128)
	f, _ := os.Open("/media/czh/sn/record/UniqueA2.txt")
	s := bufio.NewScanner(f)
	Count_T := 0
	Count_A := 0
	var shijian float64
	for s.Scan() {
		str := s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Account[Count_A] = key
		Count_A++
		if Count_A == 10100000 {
			break
		}
	}
	_ = f.Close()

	f1, _ := os.Open("/media/czh/sn/record/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0
	for s1.Scan() {
		str := s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber%8 == 0 {
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10100000 {
			break
		}
	}
	_ = f1.Close()
	number := 0
	Count2 := 0
	t1 := time.Now()
	countT:=0
	countA:=0
	for i:=0;i<5000000;i++{
		num:=rand.Intn(10000000)
		tx = Txhash[num]
		txh := BytesToHash(tx)
		GetTransaction_s(*db,txh)
		countT++
		ac = Account[num]
		tree,_ := News(root3,db)
		v := tree.Get(crypto.Keccak256(ac))
		countA++
		if v == nil{
			Count++
		}
		if i % 100000 == 0{
			log2.Println(i)
		}
	}
	t2 := time.Now()
	shijian += t2.Sub(t1).Seconds()
	TimeTx := tt1 + tt2 + tt3
	fmt.Println("总时间耗费：", shijian)
	fmt.Println("nil计数", Count, Count2, number)
	fmt.Println("kv数目,总时间，交易时间", ethdb.Count, ethdb.T, TimeTx, shijian)
	fmt.Println("qps:", float64(10000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	fmt.Println(countT,countA)
	idle1, total1 := Exper.GetCPUSample()
	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks
	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)

	runtime.GC()
}
// Test Tx:Acc Ratio is 3:7  with Normal Random Distribution
func TestMixR3(t *testing.T) {
	//	root := []byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14,
	//199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	//db,_ := ethdb.NewLDBDatabase("C:/DB/Double-P",16,1024)
	idle0, total0 := Exper.GetCPUSample()
	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Original", 16, 128)
	f, _ := os.Open("/media/czh/sn/record/UniqueA2.txt")
	s := bufio.NewScanner(f)
	Count_T := 0
	Count_A := 0
	var shijian float64
	for s.Scan() {
		str := s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Account[Count_A] = key
		Count_A++
		if Count_A == 10100000 {
			break
		}
	}
	_ = f.Close()

	f1, _ := os.Open("/media/czh/sn/record/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0
	for s1.Scan() {
		str := s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber%8 == 0 {
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10100000 {
			break
		}
	}
	_ = f1.Close()
	number := 0
	Count2 := 0
	t1 := time.Now()
	countT:=0
	countA:=0
	for i:=0;i<1000000;i++{
		for m:=0;m<3;m++{
			num:=rand.Intn(10000000)
			tx = Txhash[num]
			txh := BytesToHash(tx)
			GetTransaction_s(*db,txh)
			countT++
		}

		for n:=0;n<7;n++{
			num:=rand.Intn(10000000)
			ac = Account[num]
			tree,_ := News(root3,db)
			v := tree.Get(crypto.Keccak256(ac))
			countA++
			if v == nil{
				Count++
			}
		}

		if i % 100000 == 0{
			log2.Println(i)
		}
	}
	t2 := time.Now()
	shijian += t2.Sub(t1).Seconds()
	TimeTx := tt1 + tt2 + tt3
	fmt.Println("总时间耗费：", shijian)
	fmt.Println("nil计数", Count, Count2, number)
	fmt.Println("kv数目,总时间，交易时间", ethdb.Count, ethdb.T, TimeTx, shijian)
	fmt.Println("qps:", float64(10000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	fmt.Println(countT,countA)
	idle1, total1 := Exper.GetCPUSample()
	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks
	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)

	runtime.GC()
}
// Test Tx:Acc Ratio is 7:3  with Normal Random Distribution
func TestMixR7(t *testing.T) {
	//	root := []byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14,
	//199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	//db,_ := ethdb.NewLDBDatabase("C:/DB/Double-P",16,1024)
	idle0, total0 := Exper.GetCPUSample()
	db, _ := ethdb.NewLDBDatabase("/media/czh/sn/DB/Original", 16, 128)
	f, _ := os.Open("/media/czh/sn/record/UniqueA2.txt")
	s := bufio.NewScanner(f)
	Count_T := 0
	Count_A := 0
	var shijian float64
	for s.Scan() {
		str := s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Account[Count_A] = key
		Count_A++
		if Count_A == 10100000 {
			break
		}
	}
	_ = f.Close()

	f1, _ := os.Open("/media/czh/sn/record/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0
	for s1.Scan() {
		str := s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber%8 == 0 {
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10100000 {
			break
		}
	}
	_ = f1.Close()
	number := 0
	Count2 := 0
	t1 := time.Now()
	countT:=0
	countA:=0
	for i:=0;i<1000000;i++{
		for m:=0;m<7;m++{
			num:=rand.Intn(10000000)
			tx = Txhash[num]
			txh := BytesToHash(tx)
			GetTransaction_s(*db,txh)
			countT++
		}

		for n:=0;n<3;n++{
			num:=rand.Intn(10000000)
			ac = Account[num]
			tree,_ := News(root3,db)
			v := tree.Get(crypto.Keccak256(ac))
			countA++
			if v == nil{
				Count++
			}
		}

		if i % 100000 == 0{
			log2.Println(i)
		}
	}
	t2 := time.Now()
	shijian += t2.Sub(t1).Seconds()
	TimeTx := tt1 + tt2 + tt3
	fmt.Println("总时间耗费：", shijian)
	fmt.Println("nil计数", Count, Count2, number)
	fmt.Println("kv数目,总时间，交易时间", ethdb.Count, ethdb.T, TimeTx, shijian)
	fmt.Println("qps:", float64(10000000)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))
	fmt.Println(countT,countA)
	idle1, total1 := Exper.GetCPUSample()
	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks
	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)

	runtime.GC()
}


var(
	key2 []byte
	value2 []byte
	WriteTime2 float64
)

func TestWrite(t *testing.T) {
	db, err := leveldb.OpenFile("workload datadir", &opt.Options{
		Compression:opt.NoCompression, // none,不启用snappy压缩机制，
		WriteBuffer:50* opt.MiB,
		DisableBlockCache:true,
	})
	if err != nil {
		panic(err)
	}
	fi, err := os.Open("workload datadir")
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer db.Close()
	defer fi.Close()
	size:=0
	totali:=0
	keyi:=0
	br := bufio.NewReader(fi)
	t1:=time.Now()
	MPT:=0
	for {
		if totali%2 == 0{
			a,  c := br.ReadString('\n')
			key2, _ =hex.DecodeString(a)
			size += len(key2)
			if len(key2) == 32{
				MPT++
			}
			keyi++
			if c == io.EOF {
				break
			}
		}else{
			a,  c := br.ReadString('\n')
			value2, _ =hex.DecodeString(a)
			size += len(value2)
			if c == io.EOF {
				break
			}
			t3:=time.Now()
			if len(key2) == 32{
				_ = db.Put_s(key2, value2, nil)
			}else {
				_ = db.Put(key2, value2, nil)
			}
			t4:=time.Now()
			t:=t4.Sub(t3).Seconds()
			WriteTime2 += t
		}
		totali++
	}
	t2:=time.Now()
	db.PrintTime()
	fmt.Println("time",t2.Sub(t1).Seconds(),"put cost:",WriteTime2)
	fmt.Println("cnts:",totali,"k:",keyi,"state:",MPT)
	fmt.Println("size:",size)
	f := float64(size / 1024 / 1024)
	t := t2.Sub(t1).Seconds()
	fmt.Println("Th:",float64(f/t))
	fmt.Println(db.Getlevel0Comp())
	fmt.Println(db.Getnonlevel0Comp())
}