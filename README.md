### [Go] ChainKV: A Semantics-Awared Key-Value Store for Blockchain Systems

------

This work has been accepted by ACM SIGMOD 2024, If you can get inspiration from this work or build on this prototype for future development, please cite

~~~
@article{chen2023chainkv,
  title={ChainKV: A Semantics-Aware Key-Value Store for Ethereum System},
  author={Chen, Zehao and Li, Bingzhe and Cai, Xiaojun and Jia, Zhiping and Ju, Lei and Shao, Zili and Shen, Zhaoyan},
  journal={Proceedings of the ACM on Management of Data},
  volume={1},
  number={4},
  pages={1--23},
  year={2023},
  publisher={ACM New York, NY, USA}
}

~~~



------

ChainKV is a fast key-value storage library  specifically designed for blockchain systems.

> **This repository is receiving very limited maintenance. We will only review the following types of changes.**
>
> * Fixes for critical bugs, such as data loss or memory corruption
> * Changes absolutely needed by internally supported Geth clients. These typically fix breakage introduced by a language/standard library/OS update

### Features 

------

- It inherited all the virtues of LSM-tree based KV Store.
- Data is stored sorted by a sorted key.
- It adopts an adaptive cache strategy.
- It bridges a semantic connection between the storage layer and blockchain layer.
- It eliminates functional redundancy between different components.

### Limitations

------

- This is not a `SQL` database. It does not have a relational data model, it does not support SQL queries, and it has no support for indexes.

- Only a `single process` (possibly multi-threaded) can access a particular database at a time.

- There is no client-server support built in to the library. An application that needs such support will have to wrap their own server around the library.

### Code Organization
------
The organization of code is as follows:
~~~
---Advanced KV Store
    |--cache-sgc 		/*SGC policy*/
    |--cache-cache
    |--other files      /*leveldb*/
---Prefix MPT
	|--trie.go 			/*column prefix*/
	|--trie_test.go 	/*generate workloads*/
---Test Bench
	|--WR_test.go		/*test bench*/
~~~

If you want to re-create the project, you must build a private network to re-synchronize the historical data from the Ethereum mainnet. Then, you can adopt `trie_test.go` to re-generate the workloads. 

Before starting all test bench, you must replace the original goleveldb with the advanced KV store and allocate the data into 2 LSM-trees.

### Building Data

------

Before all tests,  you need to obtain the data sets through starting synchronization process. In my evaluation, we use `geth 1.9` client to synchronize historical data. Therefore, you can build `geth`  by a Go (version 1.16 or later) and a C compiler.

~~~shell
git clone "https://github.com/ethereum/go-ethereum"
cd go-ethereum
make geth
~~~

#### Hardware Requirements

In practice, replay blocks is limited by the performance of storage, thus, using SSD as the storage is necessary. Meanwhile, a RAM with a big capacity also can accelerate the synchronization speed.

Minimum:

- CPU with 2+ cores
- 4GB RAM
- 1TB free storage space to sync the Mainnet
- 8 MBit/sec download Internet service

Recommended:

- Fast CPU with 4+ cores
- 16GB+ RAM
- High-performance SSD with at least 1TB of free space
- 25+ MBit/sec download Internet service

#### Full node on the main Ethereum network

In my evaluation, to obtain all state transition, thus, we must synchronize all transactions in the past. 

~~~shell
geth --syncmode "full"
	 --cache ""
	 --trie.cache.gens ""
	 --datadir ""
~~~

### Usage of DB

------

- Need at least `Go1.16` or newer.

Create or open a database:

~~~shell
// The returned DB instance is safe for concurrent use. Which mean that all
// DB's methods may be called concurrently from multiple goroutine.
db, err := leveldb.OpenFile("path/to/db", nil)
...
defer db.Close()
...
~~~

Read or modify the database content:

~~~shell
// Remember that the contents of the returned slice should not be modified.
data0, err0 := db.Get([]byte("key"), nil)
data1, err1 := db.Get_s([]byte("key"), nil)
...
err0 = db.Put([]byte("key"), []byte("value"), nil)
err1 = db.Put_s([]byte("key"), []byte("value"), nil)
...
err0 = db.Delete([]byte("key"), nil)
err1 = db.Delete_s([]byte("key"), nil)
...
~~~

ChainKV mainly contains 4 new functional components:

- Partition -- State Separation
- Encoding -- Prefix MPT
- Adjustment --  Space Game Cache
- Redundancy -- Node-failure Recovery

#### State Separation

ChainKV divide the whole store space into two independent zones, including memory components  and disk components. As a result, ChainKV implement different interfaces to achieve CRUD operations. For a instance, ChainKV writes data to the two isolated zones by calling `Put()`, `Put_s()`.

```bash
// Note that the data structure involved in these two calls() are different. 
db,_ := ethdb.NewLDBDatabase("PATH")
...
db.Put(key(non-state), value(nono-state))
db.Put_s(key(state), value(state))
```

#### Prefix MPT

Prefix MPT can aggregate nodes that are strongly spatial & temporal. In practice, the Prefix MPT scheme is a encoding strategy, which can manually assign different prefixes to different KV pairs to achieve lexicographic sort of different KV pairs. See `/MPT/trie` for more details.

#### SGC

There are two cache structures in SGC for each type of data: a Real Cache and a virtual Ghost Cache. The real cache is used to cache hot KV items. The virtual ghost cache does not occupy real memory space, which only holds the metadata of the KV items evicted from the real cache. The ghost cache provides a possible hint for enlarging or squeezing the real cache. 

A hit in the ghost cache means that it could have a real cache hit if the corresponding real cache was larger. By using the ghost caches, the size of the corresponding real caches can be adjusted dynamically. Based on the data, the cache space is further subdivided into the non-state data real cache (`r`), the state data real cache (`r1`), the non-state data ghost cache (`f`), and the state data ghost cache (`f1`), respectively. 

The basic data structure is as follows.

```bash
type SGC struct{
  mu sync.Mutex								// mutex, concurrently access
  capacity int								// the sum of r and r1
  trriger int								// the target triggering the silde window 
  rused, fused, r1used, f1used int			// record the usage of r, r1, f, f1
  recent lruNode							// Header
  frequent lruNode							// Header
  r1, f1 lruNode							// Header
}
```

See `/goleveldb/leveldb/cache` for more details.

#### Lightweight Node-failure

In the lightweight node-failure recovery design, we maintain a `safe block` for both the `in-memory state memtable` and the `Non-s memtable`, and both are written to disk together with the original memtable flush operations. The purpose is to place a "marker" in the persistent storage to indicate the progress of the latest flush operation. The `safe block` is a special-purpose KV item. Its key is a pre-defined array of bytes, and its value indicates the latest block number stored in the SSTs. Therefore, to retrieve the latest successfully synchronized block number during the data recovery start-up phase, we simply query the two SST zones using the corresponding key. 

### Setup

------

We prototype ChainKV based on a public Ethereum platform.  To collect the workloads for evaluation, we built a private Ethereum environment by the following process. First, we use the Geth client to synchronize historical blockchain data to the local storage, and then we use ChainKV and other implementations to replay transactions in the synchronized Ethereum blocks. Specifically, we retrieve transaction blocks in the order of block numbers and then execute these transactions to generate and update the state data with MPT structures. Finally, we store all the data in KV pairs with the underlying storage engine.

~~~shell
ChainKV:		Version 1.0
CPU:			Intel (R) Core (TM) I7-10875 CPU @ 2.30 GHz 
Memory:			16GB memory
Disk:			1TB SN-750 NVMe SSD
OS:				Ubuntu 18.04
File System:	Ext4
~~~

#### How to use ChainKV to reproduce the experiment

- For the synchronization workload, we synchronize 4 groups of real workloads (`1.6M`, `2.3M`, `3.4M`, `4.6M` blocks).
- For the query workloads, we use 3 distributions to simulator all access behaviors.

The exact location of the code is as follows:

```bash
WRITE: Using interface() InsertChain() to replay all historail blocks
READ: All tests are located in /MPT/trie/exper_test.go
```

Before running read tests, you must botain all historial transactions hash and all accounts registered.

### Contribution

------

Thank you for considering helping out with the source code! We welcome contributions from anyone on the internet, and are grateful for even the smallest of fixes!
