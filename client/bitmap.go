package client

import (
	"errors"
	"math"
)

type Bitmap struct {
	size   int
	bitmap []byte
}

func NewBitmap(size int) Bitmap {
	return Bitmap{
		size:   size,
		bitmap: make([]byte, int(math.Ceil(float64(size)/8.0))),
	}
}

func NewBitmapWithBuf(size int, buffer []byte) Bitmap {
	return Bitmap{
		size:   size,
		bitmap: buffer,
	}
}

func (b *Bitmap) Mark(i int) error {
	if i < 0 || i >= b.size {
		return errors.New("unexpected index")
	}
	index := i / 8
	indexWithinByte := i % 8
	b.bitmap[index] |= 1 << indexWithinByte
	return nil
}

func (b *Bitmap) Get(i int) (bool, error) {
	if i < 0 || i >= b.size {
		return false, errors.New("unexpected index")
	}
	index := i / 8
	indexWithinByte := i % 8
	return bool((b.bitmap[index] & (1 << indexWithinByte)) != 0), nil
}

func (b *Bitmap) GetSize() int {
	return b.size
}

func (b *Bitmap) GetBitmap() []byte {
	return b.bitmap
}
