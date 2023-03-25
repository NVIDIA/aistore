// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package atomic

import (
	"time"
	"unsafe"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Primitive atomics tests", func() {
	It("should properly perform basic operations on Int32", func() {
		atom := NewInt32(42)

		Expect(atom.Load()).To(Equal(int32(42)))
		Expect(atom.Add(4)).To(Equal(int32(46)))
		Expect(atom.Sub(2)).To(Equal(int32(44)))
		Expect(atom.Inc()).To(Equal(int32(45)))
		Expect(atom.Dec()).To(Equal(int32(44)))

		Expect(atom.CAS(44, 0)).To(BeTrue())
		Expect(atom.Load()).To(Equal(int32(0)))

		Expect(atom.Swap(1)).To(Equal(int32(0)))
		Expect(atom.Load()).To(Equal(int32(1)))

		atom.Store(42)
		Expect(atom.Load()).To(Equal(int32(42)))
	})

	It("should properly perform basic operations on Int64", func() {
		atom := NewInt64(42)

		Expect(atom.Load()).To(Equal(int64(42)))
		Expect(atom.Add(4)).To(Equal(int64(46)))
		Expect(atom.Sub(2)).To(Equal(int64(44)))
		Expect(atom.Inc()).To(Equal(int64(45)))
		Expect(atom.Dec()).To(Equal(int64(44)))

		Expect(atom.CAS(44, 0)).To(BeTrue())
		Expect(atom.Load()).To(Equal(int64(0)))

		Expect(atom.Swap(1)).To(Equal(int64(0)))
		Expect(atom.Load()).To(Equal(int64(1)))

		atom.Store(42)
		Expect(atom.Load()).To(Equal(int64(42)))
	})

	It("should properly perform basic operations on Uint32", func() {
		atom := NewUint32(42)

		Expect(atom.Load()).To(Equal(uint32(42)))
		Expect(atom.Add(4)).To(Equal(uint32(46)))
		Expect(atom.Sub(2)).To(Equal(uint32(44)))
		Expect(atom.Inc()).To(Equal(uint32(45)))
		Expect(atom.Dec()).To(Equal(uint32(44)))

		Expect(atom.CAS(44, 0)).To(BeTrue())
		Expect(atom.Load()).To(Equal(uint32(0)))

		Expect(atom.Swap(1)).To(Equal(uint32(0)))
		Expect(atom.Load()).To(Equal(uint32(1)))

		atom.Store(42)
		Expect(atom.Load()).To(Equal(uint32(42)))
	})

	It("should properly perform basic operations on Uint64", func() {
		atom := NewUint64(42)

		Expect(atom.Load()).To(Equal(uint64(42)))
		Expect(atom.Add(4)).To(Equal(uint64(46)))
		Expect(atom.Sub(2)).To(Equal(uint64(44)))
		Expect(atom.Inc()).To(Equal(uint64(45)))
		Expect(atom.Dec()).To(Equal(uint64(44)))

		Expect(atom.CAS(44, 0)).To(BeTrue())
		Expect(atom.Load()).To(Equal(uint64(0)))

		Expect(atom.Swap(1)).To(Equal(uint64(0)))
		Expect(atom.Load()).To(Equal(uint64(1)))

		atom.Store(42)
		Expect(atom.Load()).To(Equal(uint64(42)))
	})

	It("should properly perform basic operations on Bool", func() {
		atom := NewBool(false)
		Expect(atom.Toggle()).To(BeFalse())
		Expect(atom.Load()).To(BeTrue())

		Expect(atom.CAS(true, true)).To(BeTrue())
		Expect(atom.Load()).To(BeTrue())
		Expect(atom.CAS(true, false)).To(BeTrue())
		Expect(atom.Load()).To(BeFalse())
		Expect(atom.CAS(true, false)).To(BeFalse())
		Expect(atom.Load()).To(BeFalse())

		atom.Store(false)
		Expect(atom.Load()).To(BeFalse())

		prev := atom.Swap(false)
		Expect(prev).To(BeFalse())

		prev = atom.Swap(true)
		Expect(prev).To(BeFalse())
	})

	It("should properly perform basic operations on Duration", func() {
		atom := NewDuration(5 * time.Minute)

		Expect(atom.Load()).To(Equal(5 * time.Minute))
		Expect(atom.Add(time.Minute)).To(Equal(6 * time.Minute))
		Expect(atom.Sub(2 * time.Minute)).To(Equal(4 * time.Minute))

		Expect(atom.CAS(4*time.Minute, time.Minute)).To(BeTrue())
		Expect(atom.Load()).To(Equal(time.Minute))

		Expect(atom.Swap(2 * time.Minute)).To(Equal(time.Minute))
		Expect(atom.Load()).To(Equal(2 * time.Minute))

		atom.Store(10 * time.Minute)
		Expect(atom.Load()).To(Equal(10 * time.Minute))
	})

	It("should properly perform basic operations on Time", func() {
		now := time.Now()
		atom := NewTime(now)
		Expect(atom.Load()).Should(BeTemporally("~", now, 0))
	})

	It("should properly perform basic operations on Value", func() {
		var v Value
		Expect(v.Load()).To(BeNil())

		v.Store(42)
		Expect(v.Load()).To(Equal(42))

		v.Store(84)
		Expect(v.Load()).To(Equal(84))

		Expect(func() { v.Store("abc") }).Should(Panic())
	})

	It("should properly perform basic operations on Pointer", func() {
		var v Pointer
		Expect(v.Load()).To(Equal(unsafe.Pointer(nil)))

		var x int = 42
		v.Store(unsafe.Pointer(&x))
		Expect((*int)(v.Load())).To(Equal(&x))

		var y int = 84
		v.Store(unsafe.Pointer(&y))
		loadedY := (*int)(v.Load())
		Expect(loadedY).To(Equal(&y))
		*loadedY = 0

		Expect(x).To(Equal(42))
		Expect(y).To(Equal(0))
	})
})
