// Copyright 2016 The go-ethereum Authors
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
	"bytes"
	"fmt"
	"hash"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/rlp"
)

type hasher struct {
	tmp                  *bytes.Buffer
	sha                  hash.Hash
	cachegen, cachelimit uint16
}

// hashers live in a global pool.
var hasherPool = sync.Pool{
	New: func() interface{} {
		return &hasher{tmp: new(bytes.Buffer), sha: sha3.NewKeccak256()}
	},
}

func newHasher(cachegen, cachelimit uint16) *hasher {
	h := hasherPool.Get().(*hasher)
	h.cachegen, h.cachelimit = cachegen, cachelimit
	return h
}

func returnHasherToPool(h *hasher) {
	hasherPool.Put(h)
}

// hash collapses a node down into a hash node, also returning a copy of the
// original node initialized with the computed hash to replace the original one.

// hash方法需要一个参数来接受hasChildren方法调用hash时，node的Prefix
func (h *hasher) hash(n node, db DatabaseWriter, p []byte, force bool) (node, node, error) {
	// If we're not storing the node, just hashing, use available cached data
	// 尝试使用cache的哈希值，其中新创建的node的dirty被赋值为为true，一般从root开始
	// cache返回node的hash和dirty
	if hash, dirty := n.cache(); hash != nil {
		if db == nil {
			return hash, n, nil
		}
		// node的缓存机制，用cachegen计数，每次CommitTo后，node都会自增1
		// 若连续执行CommitTo（cachelimit==120）次未命中，则节点被折叠为hash
		// 如果node被改变了，那么则会形成新的node，且gen会发生变化，就如同新的一样
		if n.canUnload(h.cachegen, h.cachelimit) {
			// Unload the node from cache. All of its subnodes will have a lower or equal cache generation number.
			// canUnload返回n.flags.canUnload(gen, limit)返回!n.dirty && cachegen-n.gen >= cachelimit，当dirty为false意为
			// node不是新建，且node的寿命到了，已经被折叠为hash
			cacheUnloadCounter.Inc(1) // +1？ metric包
			return hash, hash, nil
		}
		// 否则，直接返回hash和node
		if !dirty {
			return hash, n, nil
		}
	}
	// 需要存储，开始遍历子节点，把原有的子节点替换成子节点的hash值
	// 计算原有hash值的主要流程是首先调用h.hashChildren(n,db)把所有的子节点的hash值求出来，把原有的子节点替换成子节点的hash值。
	// 这是一个递归调用的过程，会从树叶依次往上计算直到树根。然后调用store方法计算当前节点的hash值，并把当前节点的hash值放入
	// cache节点，设置dirty参数为false(新创建的节点的dirty值是为true的)，然后返回。

	//返回值说明， cache变量包含了原有的node节点，并且包含了node节点的hash值。 hash变量返回了当前节点的hash值(这个值其实
	// 是根据node和node的所有子节点计算出来的)。

	//有一个小细节： 根节点调用hash函数的时候， force参数是为true的，其他的子节点调用的时候force参数是为false的。
	// force参数的用途是当||c(J,i)||<32的时候也对c(J,i)进行hash计算，这样保证无论如何也会对根节点进行Hash计算。

	// n为本func中的node，collapsed、cached是其本节点的copy，要注意collapsed用作store，所以会Hex转为Compact
	// 如果n没有没有子节点了，则在hashChildren中返回继续执行store，如果由子节点，则在hashChildren中调用hash继续递归遍历。
	// 获取当前节点的collapsed、cache和prefix，留作store使用
	collapsed, cached, prefix,err := h.hashChildren(n, p, db)
	if err != nil {
		return hashNode{}, n, err
	}
	// 当前节点的hash值，调用store把kv对写入数据库（ETH调用处是写进batch中）
	// hashed是已经加了前缀之后的
	hashed, err := h.store(collapsed, db, prefix, force)
	if err != nil {
		return hashNode{}, n, err
	}
	// Cache the hash of the node for later reuse and remove
	// the dirty flag in commit mode. It's fine to assign these values directly
	// without copying the node first because hashChildren copies it.
	// 首先缓存node的hash值，即为记录到flag中；因为在入库之后node已经被提交，所以将dirty改为false
	cachedHash, _ := hashed.(hashNode)
	switch cn := cached.(type) {
	case *shortNode:
		cn.flags.hash = cachedHash
		if db != nil {
			cn.flags.dirty = false
		}
	case *fullNode:
		cn.flags.hash = cachedHash
		if db != nil {
			cn.flags.dirty = false
		}
	}
	return hashed, cached, nil
}

// hashChildren replaces the children of a node with their hashes if the encoded
// size of the child is larger than a hash, returning the collapsed node as well
// as a replacement for the original node with the child hashes cached in.
// cache变量接管了原来的Trie树的完整结构，collapsed变量把子节点替换成子节点的hash值

/// 为了应对Add Prefix，hashChildren方法需要多一个参数来表示prefix
func (h *hasher) hashChildren(original node, p []byte, db DatabaseWriter) (node, node,[]byte, error) {
	var err error
	//var num byte
	//if p == nil{
	//	num = 0
	//} else {
	//	num = p[0]
	//}
	//Num := int(num)
	//Num ++
	//p = []byte{byte(Num)}
	switch n := original.(type) {
	case *shortNode:
		// Hash the short node's child, caching the newly hashed subtree
		collapsed, cached := n.copy(), n.copy()
		// 将Hex编码转为Compact编码,用于存储到磁盘
		collapsed.Key = hexToCompact(n.Key)
		// cached.Key仍为Hex编码
		cached.Key = common.CopyBytes(n.Key)
		// p是前缀
		//p = append(p,cached.Key...) //nil+0102030
		//p = append(p,nil...)

		// 如果不是叶子结点(valueNode)，则需要递归调用回hash，ok用来判断是否是valuenode类型，是则ok为true，否则为fasle
		if _, ok := n.Val.(valueNode); !ok {
			// 递归调用hash，参数为n的孩子，因为是shortNode，孩子只有一个就是n.Val
			collapsed.Val, cached.Val, err = h.hash(n.Val, db, p, false)
			if err != nil {
				return original, original, p, err
			}
		}
		if collapsed.Val == nil {
			// 确保空子节点能够被编码为nil
			collapsed.Val = valueNode(nil) // Ensure that nil children are encoded as empty strings.
		}
		// 返回collapsed(Compact Encoding)和cached(原来的的结构),以及此节点的Prefix
		return collapsed, cached, p,nil
	// 当前节点是fullNode, 那么遍历每个子节点，把每个子节点替换成子节点的Hash值(使用hash递归)，
	case *fullNode:
		// Hash the full node's children, caching the newly hashed subtrees
		collapsed, cached := n.copy(), n.copy()
		//prefix:=p
		for i := 0; i < 16; i++ {
			// 如果Children[i]不为空，说明本节点仍然有子节点，则将其子节点作为参数，递归调用hash()
			if n.Children[i] != nil {
				// 同理，collapsd、cached都是本node的copy，collapsed中的Key经过RLP编码
				//PrefixFullNode := []byte{byte(i)}
				//p = append(prefix, PrefixFullNode...)
				collapsed.Children[i], cached.Children[i], err = h.hash(n.Children[i], db, p, false)
				if err != nil {
					return original, original, p ,err
				}
			} else {
				collapsed.Children[i] = valueNode(nil) // Ensure that nil children are encoded as empty strings.
			}
		}
		cached.Children[16] = n.Children[16] //Children[16]存放的是valueNode
		if collapsed.Children[16] == nil {
			collapsed.Children[16] = valueNode(nil) // Ensure that nil children are encoded as empty strings.
		}
		return collapsed, cached, p, nil

	default:
		// 没有子节点，直接返回
		// Value and hash nodes don't have children so they're left as were
		return n, original, nil,nil
	}
}
// store方法，如果一个node的所有子节点都替换成了子节点的hash值，那么直接调用rlp.Encode方法对这个节点进行编码，
// 如果编码后的值小于32， 并且这个节点不是根节点，那么就把他们直接存储在他们的父节点里面，否者调用h.sha.Write方法
// 进行hash计算，然后把hash值和编码后的数据存储到数据库里面，然后返回hash值。
// 可以看到每个值大于32的节点的值和hash都存储到了数据库里面。
//var f, err = os.OpenFile("E:/DB/200/StateKP2.txt",os.O_WRONLY | os.O_CREATE | os.O_APPEND,0666)
// 为了应对Add Prefix，store方法需要多一个参数来表示prefix
func (h *hasher) store(n node, db DatabaseWriter, p []byte,force bool) (node, error) {
	// Don't store hashes or empty nodes.
	// 已经折叠过的或者为空的，不存储？
	if _, isHash := n.(hashNode); n == nil || isHash {
		return n, nil
	}
	// Generate the RLP encoding of the node
	// 对node进行RLP编码
	h.tmp.Reset()
	if err := rlp.Encode(h.tmp, n); err != nil {
		panic("encode error: " + err.Error())
	}
	//莫非说的是节点为空的时候？
	if h.tmp.Len() < 32 && !force {
		return n, nil // Nodes smaller than 32 bytes are stored inside their parent
	}
	// Larger nodes are replaced by their hash and stored in the database.
	// 如果cache中hash为nil，说明没有进行过hash计算，进行hash计算，
	// key：hashNode(h.sha.Sum(nil)) --> value：h.tmp.Bytes()
	hash, _ := n.cache()
	if hash == nil {
		h.sha.Reset()
		h.sha.Write(h.tmp.Bytes())
		//h.sha.Sum为hash计算后的32字节哈希值
		hash = hashNode(h.sha.Sum(nil))
	}
	// 对node哈希计算之后，再加前缀，然后再把哈希值传递给父节点
	if db != nil {
		switch n.(type){
		case *shortNode:
			Prefix:=hexToKeybytes2(p)
			hash=append(Prefix,h.sha.Sum(nil)...)
			//fmt.Println("This is shortNode.")
			//fmt.Println("PrefixKey：",hash,len(hash))
		case *fullNode:
			Prefix:=hexToKeybytes2(p)
			hash=append(Prefix,h.sha.Sum(nil)...)
			//fmt.Println("This is fullNode.")
			//fmt.Println("PrefixKey：",hash,len(hash))
		default:
			fmt.Println("Something is wrong!")
		}
		//fmt.Println("Key：",h.sha.Sum(nil),"Value：",h.tmp.Bytes())
		//fmt.Println("Key：",h.sha.Sum(nil))
		//fmt.Println("Value：",h.tmp.Bytes())
		//PrefixNode:=append()
		//str:=hex.EncodeToString(hash)
		//_, _ = fmt.Fprintln(f, str)
		return hash, db.Put(hash, h.tmp.Bytes())
		//return hash, db.Put(hash, h.tmp.Bytes())
	}
	return hash, nil
}
