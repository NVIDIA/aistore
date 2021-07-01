// Package xoshiro256 implements the xoshiro256** RNG
// no-copyright
/*
Translated from
	http://xoshiro.di.unimi.it/xoshiro256starstar.c
	Scrambled Linear Pseudorandom Number Generators
	David Blackman, Sebastiano Vigna
	https://arxiv.org/abs/1805.01407
	http://www.pcg-random.org/posts/a-quick-look-at-xoshiro256.html
*/
package xoshiro256

func Hash(seed uint64) uint64 {
	const n = 64

	// Recommendation as per http://xoshiro.di.unimi.it/xoshiro256starstar.c:
	//
	// The state must be seeded so that it is not everywhere zero. If you have
	// a 64-bit seed, we suggest to seed a splitmix64 generator and use its
	// output to fill s.

	z := seed + 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	z = (z ^ (z >> 31)) + 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	z = (z ^ (z >> 31)) * 5

	// inlined: bits.RotateLeft64(z, 7) * 9
	s := uint(7) & (n - 1)
	return (z<<s | z>>(n-s)) * 9
}
