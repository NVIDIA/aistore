# Cuckoo Filter

[![GoDoc](https://godoc.org/github.com/seiflotfy/cuckoofilter?status.svg)](https://godoc.org/github.com/seiflotfy/cuckoofilter) [![CodeHunt.io](https://img.shields.io/badge/vote-codehunt.io-02AFD1.svg)](http://codehunt.io/sub/cuckoo-filter/?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

Cuckoo filter is a Bloom filter replacement for approximated set-membership queries. While Bloom filters are well-known space-efficient data structures to serve queries like "if item x is in a set?", they do not support deletion. Their variances to enable deletion (like counting Bloom filters) usually require much more space.

Cuckoo ﬁlters provide the ﬂexibility to add and remove items dynamically. A cuckoo filter is based on cuckoo hashing (and therefore named as cuckoo filter). It is essentially a cuckoo hash table storing each key's fingerprint. Cuckoo hash tables can be highly compact, thus a cuckoo filter could use less space than conventional Bloom ﬁlters, for applications that require low false positive rates (< 3%).

For details about the algorithm and citations please use this article for now

["Cuckoo Filter: Better Than Bloom" by Bin Fan, Dave Andersen and Michael Kaminsky](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf)

## Note
This implementation uses a a static bucket size of 4 fingerprints and a fingerprint size of 1 byte based on my understanding of an optimal bucket/fingerprint/size ratio from the aforementioned paper.

## Example usage:
```go
package main

import "fmt"
import "github.com/seiflotfy/cuckoofilter"

func main() {
  cf := cuckoo.NewFilter(1000)
  cf.InsertUnique([]byte("geeky ogre"))

  // Lookup a string (and it a miss) if it exists in the cuckoofilter
  cf.Lookup([]byte("hello"))

  count := cf.Count()
  fmt.Println(count) // count == 1

  // Delete a string (and it a miss)
  cf.Delete([]byte("hello"))

  count = cf.Count()
  fmt.Println(count) // count == 1

  // Delete a string (a hit)
  cf.Delete([]byte("geeky ogre"))

  count = cf.Count()
  fmt.Println(count) // count == 0
  
  cf.Reset()    // reset
}
```

## Documentation:
["Cuckoo Filter on GoDoc"](http://godoc.org/github.com/seiflotfy/cuckoofilter)
