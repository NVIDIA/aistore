package cuckoo

type bucket [bucketSize]byte

const (
	nullFp     = 0
	bucketSize = 4
)

func (b *bucket) insert(fp byte) bool {
	for i, tfp := range b {
		if tfp == nullFp {
			b[i] = fp
			return true
		}
	}
	return false
}

func (b *bucket) delete(fp byte) bool {
	for i, tfp := range b {
		if tfp == fp {
			b[i] = nullFp
			return true
		}
	}
	return false
}

func (b *bucket) getFingerprintIndex(fp byte) int {
	for i, tfp := range b {
		if tfp == fp {
			return i
		}
	}
	return -1
}

func (b *bucket) reset() {
	for i := range b {
		b[i] = nullFp
	}
}
