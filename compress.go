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
)

const (
	compHeaderSize = 7
)

// compIOZlibPool is a bucket of compIO pools using zlib compression, one for each compression level
type compIOZlibPool struct {
	pools [zlib.BestCompression + 1]sync.Pool
}

var (
	zlibPool    compIOZlibPool
	blankHeader []byte
)

func init() {
	blankHeader = make([]byte, compHeaderSize)
	zlibPool = *newZlibPool()
}

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

func newZlibPool() *compIOZlibPool {
	p := &compIOZlibPool{}
	for i := range p.pools {
		var err error
		level := i
		p.pools[i].New = func() any {
			c := &compIO{}
			c.mc = nil
			c.buff = bytes.Buffer{}
			c.zw, err = zlib.NewWriterLevel(&c.buff, level)
			if err != nil {
				panic(err)
			}
			c.zr = nil
			return c
		}
	}
	return p
}

func (c *compIO) zDecompress(src []byte) (int, error) {
	br := bytes.NewReader(src)
	var err error
	if c.zr == nil {
		c.zr, err = zlib.NewReader(br)
		if err != nil {
			return 0, err
		}
	} else {
		err = c.zr.(zlib.Resetter).Reset(br, nil)
		if err != nil {
			return 0, err
		}
	}
	n, _ := c.buff.ReadFrom(c.zr) // ignore err because zr.Close() will return it again.
	err = c.zr.Close()            // zr.Close() may return chuecksum error.
	return int(n), err
}

func (c *compIO) zCompress(src []byte) error {
	var err error
	c.zw.Reset(&c.buff)
	if _, err := c.zw.Write(src); err != nil {
		return err
	}
	err = c.zw.Close()
	return err
}

type compIO struct {
	mc   *mysqlConn
	buff bytes.Buffer
	zw   *zlib.Writer
	zr   io.ReadCloser
}

func newCompIO(mc *mysqlConn) *compIO {
	c, ok := zlibPool.pools[mc.cfg.compressLevel].Get().(*compIO)
	if !ok {
		panic(fmt.Sprintf("unexpected type %T", c))
	}
	c.mc = mc
	return c
}

func (c *compIO) close() {
	level := c.mc.cfg.compressLevel
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
	header, err := c.mc.readNext(7)
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
	nread, err := c.zDecompress(comprData)
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
		buf.Write(blankHeader) // Buffer.Write() never returns error

		// If payload is less than minCompressLength, don't compress.
		if header.UncompressedLength < minCompressLength {
			buf.Write(payload)
			header.UncompressedLength = 0
		} else {
			err := c.zCompress(payload)
			if debug && err != nil {
				fmt.Printf("zCompress error: %v", err)
			}
			// do not compress if compressed data is larger than uncompressed data
			// I intentionally miss 7 byte header in the buf; zCompress must compress more than 7 bytes.
			if err != nil || buf.Len() >= header.UncompressedLength {
				buf.Reset()
				buf.Write(blankHeader)
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
