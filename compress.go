// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2024 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zlib"
	"github.com/klauspost/compress/zstd"
)

const (
	compHeaderSize = 7
)

// compIOZlibPool is a bucket of compIO pools using zlib compression, one for each compression level
type compIOZlibPool struct {
	pools [zlib.BestCompression + 1]sync.Pool
}

// compIOZstdPool is a bucket of compIO pools using zstd compression, one for each compression level
type compIOZstdPool struct {
	pools [zstd.SpeedBestCompression + 1]sync.Pool
}

var (
	inintzlibPool sync.Once
	inintzstdPool sync.Once
	zlibPool      *compIOZlibPool
	zstdPool      *compIOZstdPool
	blankHeader   = [compHeaderSize]byte{}
)

// compHeader holds the data of a MySQl compressed packet header
type compHeader struct {
	CompressedLength   int
	Sequence           uint8
	UncompressedLength int
}

func (h *compHeader) read(data []byte) {
	h.CompressedLength = getUint24(data[0:3])
	h.Sequence = data[3]
	h.UncompressedLength = getUint24(data[4:7])
}

func (h *compHeader) write(data []byte) {
	putUint24(data[0:3], h.CompressedLength)
	data[3] = h.Sequence
	putUint24(data[4:7], h.UncompressedLength)
}

type compressor interface {
	compress(src []byte, dst io.Writer) error
	uncompress(src []byte, dst io.ReaderFrom) (int, error)
}

type zlibCompressor struct {
	writer *zlib.Writer
	reader io.ReadCloser
}

func (zl *zlibCompressor) compress(src []byte, dst io.Writer) error {
	var err error
	zl.writer.Reset(dst)
	if _, err := zl.writer.Write(src); err != nil {
		return err
	}
	err = zl.writer.Close()
	return err

}

func (zl *zlibCompressor) uncompress(src []byte, dst io.ReaderFrom) (int, error) {
	br := bytes.NewReader(src)
	var err error
	if zl.reader == nil {
		zl.reader, err = zlib.NewReader(br)
		if err != nil {
			return 0, err
		}
	} else {
		err = zl.reader.(zlib.Resetter).Reset(br, nil)
		if err != nil {
			return 0, err
		}
	}
	n, _ := dst.ReadFrom(zl.reader) // ignore err because reder.Close() will return it again.
	err = zl.reader.Close()         // reader.Close() may return chuecksum error.
	return int(n), err
}

func newZlibPool() *compIOZlibPool {
	p := &compIOZlibPool{}
	for i := range p.pools {
		level := i
		p.pools[i].New = func() any {
			c := &compIO{}
			c.buff = bytes.Buffer{}
			c.mc = nil
			writer, err := zlib.NewWriterLevel(&c.buff, level)
			if err != nil {
				panic(err)
			}
			c.comp = &zlibCompressor{writer: writer, reader: nil}
			return c
		}
	}
	return p
}

type zstdCompressor struct {
	writer *zstd.Encoder
	reader io.Reader
}

func (zs *zstdCompressor) compress(src []byte, dst io.Writer) error {
	var err error
	zs.writer.Reset(dst)
	if _, err := zs.writer.Write(src); err != nil {
		return err
	}
	err = zs.writer.Close()
	return err
}

func (zs *zstdCompressor) uncompress(src []byte, dst io.ReaderFrom) (int, error) {
	br := bytes.NewReader(src)
	var err error
	if zs.reader == nil {
		zs.reader, err = zstd.NewReader(br)
		if err != nil {
			return 0, err
		}
	} else {
		err = zs.reader.(*zstd.Decoder).Reset(br)
		if err != nil {
			return 0, err
		}
	}
	n, err := dst.ReadFrom(zs.reader)
	zs.reader.(*zstd.Decoder).Reset(nil)
	return int(n), err
}

func newZstdPool() *compIOZstdPool {
	p := &compIOZstdPool{}
	for i := range p.pools {
		level := i
		p.pools[i].New = func() any {
			c := &compIO{}
			c.buff = bytes.Buffer{}
			c.mc = nil
			writer, err := zstd.NewWriter(&c.buff, zstd.WithEncoderLevel(zstd.EncoderLevel(level)))
			if err != nil {
				panic(err)
			}
			c.comp = &zstdCompressor{writer: writer, reader: nil}
			return c
		}
	}
	return p
}

type compIO struct {
	buff bytes.Buffer
	mc   *mysqlConn
	comp compressor
}

func newCompIO(mc *mysqlConn) *compIO {
	if mc.cfg.zstdCompress {
		inintzstdPool.Do(func() {
			zstdPool = newZstdPool()
		})
		c, ok := zstdPool.pools[zstd.EncoderLevelFromZstd(mc.cfg.zstdCompressionLevel)].Get().(*compIO)
		if !ok {
			panic(fmt.Sprintf("unexpected zstdPool type %T", c))
		}
		c.mc = mc
		return c
	}
	inintzlibPool.Do(func() {
		zlibPool = newZlibPool()
	})
	c, ok := zlibPool.pools[mc.cfg.zlibCompressionLevel].Get().(*compIO)
	if !ok {
		panic(fmt.Sprintf("unexpected zlibPool type %T", c))
	}
	c.mc = mc
	return c
}

func (c *compIO) close() {
	if c.mc.cfg.zstdCompress {
		level := zstd.EncoderLevelFromZstd(c.mc.cfg.zlibCompressionLevel)
		c.mc = nil
		c.reset()
		zstdPool.pools[level].Put(c)
		return
	}
	level := c.mc.cfg.zlibCompressionLevel
	c.mc = nil
	c.reset()
	zlibPool.pools[level].Put(c)
}

func (c *compIO) reset() {
	c.buff.Reset()
}

func (c *compIO) readNext(need int) ([]byte, error) {
	for c.buff.Len() < need {
		if err := c.readCompressedPacket(); err != nil {
			return nil, err
		}
	}
	data := c.buff.Next(need)
	return data[:need:need], nil // prevent caller writes into c.buff
}

func (c *compIO) readCompressedPacket() error {
	header, err := c.mc.readNext(compHeaderSize)
	if err != nil {
		return err
	}
	_ = header[6] // bounds check hint to compiler; guaranteed by readNext

	// Read compressed header data
	h := &compHeader{}
	h.read(header)
	if debug {
		fmt.Printf("uncompress cmplen=%v uncomplen=%v pkt_cmp_seq=%v expected_cmp_seq=%v\n",
			h.CompressedLength, h.UncompressedLength, h.Sequence, c.mc.sequence)
	}
	// Do not return ErrPktSync here.
	// Server may return error packet (e.g. 1153 Got a packet bigger than 'max_allowed_packet' bytes)
	// before receiving all packets from client. In this case, seqnr is younger than expected.
	// NOTE: Both of mariadbclient and mysqlclient do not check seqnr. Only server checks it.
	if debug && h.Sequence != c.mc.compressSequence {
		fmt.Printf("WARN: unexpected cmpress seq nr: expected %v, got %v",
			c.mc.compressSequence, h.Sequence)
	}
	c.mc.compressSequence = h.Sequence + 1

	comprData, err := c.mc.readNext(h.CompressedLength)
	if err != nil {
		return err
	}

	// if payload is uncompressed, its length will be specified as zero, and its
	// true length is contained in comprLength
	if h.UncompressedLength == 0 {
		c.buff.Write(comprData)
		return nil
	}

	// use existing capacity in bytesBuf if possible
	c.buff.Grow(h.UncompressedLength)
	nread, err := c.comp.uncompress(comprData, &c.buff)
	if err != nil {
		return err
	}
	if nread != h.UncompressedLength {
		return fmt.Errorf("invalid compressed packet: uncompressed length in header is %d, actual %d",
			h.UncompressedLength, nread)
	}
	return nil
}

const minCompressLength = 150
const maxPayloadLen = maxPacketSize - 4

// writePackets sends one or some packets with compression.
// Use this instead of mc.netConn.Write() when mc.compress is true.
func (c *compIO) writePackets(packets []byte) (int, error) {
	totalBytes := len(packets)
	buf := &c.buff

	for len(packets) > 0 {
		header := &compHeader{}
		payloadLen := min(maxPayloadLen, len(packets))
		payload := packets[:payloadLen]
		header.UncompressedLength = payloadLen

		buf.Reset()
		buf.Write(blankHeader[:]) // Buffer.Write() never returns error

		// If payload is less than minCompressLength, don't compress.
		if header.UncompressedLength < minCompressLength {
			buf.Write(payload)
			header.UncompressedLength = 0
		} else {
			err := c.comp.compress(payload, buf)
			if debug && err != nil {
				fmt.Printf("zCompress error: %v", err)
			}
			// do not compress if compressed data is larger than uncompressed data
			// I intentionally miss 7 byte header in the buf; zCompress must compress more than 7 bytes.
			if err != nil || buf.Len() >= header.UncompressedLength {
				buf.Reset()
				buf.Write(blankHeader[:])
				buf.Write(payload)
				header.UncompressedLength = 0
			}
		}
		header.CompressedLength = buf.Len() - compHeaderSize
		header.Sequence = c.mc.compressSequence

		if n, err := c.writeCompressedPacket(header); err != nil {
			// To allow returning ErrBadConn when sending really 0 bytes, we sum
			// up compressed bytes that is returned by underlying Write().
			return totalBytes - len(packets) + n, err
		}
		packets = packets[payloadLen:]
	}

	return totalBytes, nil
}

// writeCompressedPacket writes a compressed packet with header.
// data should start with 7 size space for header followed by payload.
func (c *compIO) writeCompressedPacket(header *compHeader) (int, error) {
	data := c.buff.Bytes()
	header.write(data)
	if debug {
		fmt.Printf(
			"writeCompressedPacket: comprLength=%v, uncompressedLen=%v, seq=%v\n",
			header.CompressedLength, header.UncompressedLength, header.Sequence)
	}
	c.mc.compressSequence++
	return c.mc.writeWithTimeout(data)
}
