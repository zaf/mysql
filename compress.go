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

var (
	blankHeader = [compHeaderSize]byte{}

	// Shared codec pools: encoders/decoders are borrowed per-operation
	zlibWriterPools  [zlib.BestCompression + 1]sync.Pool
	zlibReaderPool   sync.Pool
	zstdEncoderPools [zstd.SpeedBestCompression + 1]sync.Pool
	zstdDecoderPool  sync.Pool
)

// compHeader holds the data of a MySQL compressed packet header
type compHeader struct {
	CompressedLength   uint32
	UncompressedLength uint32
	Sequence           uint8
}

func (h *compHeader) read(data []byte) {
	h.CompressedLength = uint32(getUint24(data[0:3]))
	h.Sequence = data[3]
	h.UncompressedLength = uint32(getUint24(data[4:7]))
}

func (h *compHeader) write(data []byte) {
	putUint24(data[0:3], int(h.CompressedLength))
	data[3] = h.Sequence
	putUint24(data[4:7], int(h.UncompressedLength))
}

// compressor interface
type compressor interface {
	compress(src []byte, dst io.Writer) error
	uncompress(src []byte, dst io.ReaderFrom) (int, error)
	// releaseCodecs returns heavy encoder/decoder objects to shared pools
	// and trims oversized scratch buffers. Called when the connection is closed.
	releaseCodecs()
}

// zlibCompressor implements the compressor interface using zlib.
// The heavy zlib.Writer and zlib.Reader are lazily acquired from shared
// pools on first use and released back between queries via releaseCodecs.
type zlibCompressor struct {
	level      int
	writer     *zlib.Writer  // nil when idle; acquired on first compress
	reader     io.ReadCloser // nil when idle; acquired on first uncompress
	buffReader *bytes.Reader
}

func (zl *zlibCompressor) compress(src []byte, dst io.Writer) error {
	if zl.writer == nil {
		w, ok := zlibWriterPools[zl.level].Get().(*zlib.Writer)
		if !ok {
			var err error
			w, err = zlib.NewWriterLevel(dst, zl.level)
			if err != nil {
				return err
			}
		} else {
			w.Reset(dst)
		}
		zl.writer = w
	} else {
		zl.writer.Reset(dst)
	}
	if _, err := zl.writer.Write(src); err != nil {
		return err
	}
	return zl.writer.Close()
}

func (zl *zlibCompressor) uncompress(src []byte, dst io.ReaderFrom) (int, error) {
	if zl.buffReader == nil {
		zl.buffReader = bytes.NewReader(src)
	} else {
		zl.buffReader.Reset(src)
	}
	if zl.reader == nil {
		r, ok := zlibReaderPool.Get().(io.ReadCloser)
		if ok {
			if err := r.(zlib.Resetter).Reset(zl.buffReader, nil); err != nil {
				zlibReaderPool.Put(r)
				return 0, err
			}
		} else {
			var err error
			r, err = zlib.NewReader(zl.buffReader)
			if err != nil {
				return 0, err
			}
		}
		zl.reader = r
	} else {
		if err := zl.reader.(zlib.Resetter).Reset(zl.buffReader, nil); err != nil {
			return 0, err
		}
	}
	n, _ := dst.ReadFrom(zl.reader) // ignore err because reader.Close() will return it again.
	err := zl.reader.Close()        // reader.Close() may return checksum error.
	return int(n), err
}

func (zl *zlibCompressor) releaseCodecs() {
	if zl.writer != nil {
		zlibWriterPools[zl.level].Put(zl.writer)
		zl.writer = nil
	}
	if zl.reader != nil {
		zlibReaderPool.Put(zl.reader)
		zl.reader = nil
	}
}

// zstdCompressor implements the compressor interface using zstd.
// The heavy zstd.Encoder and zstd.Decoder are lazily acquired from shared
// pools on first use and released back between queries via releaseCodecs.
type zstdCompressor struct {
	level      zstd.EncoderLevel
	encoder    *zstd.Encoder // nil when idle; acquired on first compress
	decoder    *zstd.Decoder // nil when idle; acquired on first uncompress
	scratch    []byte
	buffReader *bytes.Reader
}

func (zs *zstdCompressor) compress(src []byte, dst io.Writer) error {
	if zs.encoder == nil {
		enc, ok := zstdEncoderPools[zs.level].Get().(*zstd.Encoder)
		if !ok {
			enc = newZstdEncoder(zs.level)
		}
		zs.encoder = enc
	}
	compressed := zs.encoder.EncodeAll(src, zs.scratch[:0])
	zs.scratch = compressed // reuse backing array on next call
	_, err := dst.Write(compressed)
	return err
}

func (zs *zstdCompressor) uncompress(src []byte, dst io.ReaderFrom) (int, error) {
	if zs.decoder == nil {
		dec, ok := zstdDecoderPool.Get().(*zstd.Decoder)
		if !ok {
			dec = newZstdDecoder()
		}
		zs.decoder = dec
	}
	decoded, err := zs.decoder.DecodeAll(src, zs.scratch[:0])
	if err != nil {
		return 0, err
	}
	zs.scratch = decoded // reuse backing array on next call
	if zs.buffReader == nil {
		zs.buffReader = bytes.NewReader(decoded)
	} else {
		zs.buffReader.Reset(decoded)
	}
	n, err := dst.ReadFrom(zs.buffReader)
	return int(n), err
}

func (zs *zstdCompressor) releaseCodecs() {
	if cap(zs.scratch) > maxIdleBufferSize {
		zs.scratch = nil
	}
	if zs.encoder != nil {
		zstdEncoderPools[zs.level].Put(zs.encoder)
		zs.encoder = nil
	}
	if zs.decoder != nil {
		zstdDecoderPool.Put(zs.decoder)
		zs.decoder = nil
	}
}

func newZstdEncoder(level zstd.EncoderLevel) *zstd.Encoder {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(level),
		zstd.WithLowerEncoderMem(true),
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(1<<18), // 256KB
	)
	if err != nil {
		panic(err)
	}
	return enc
}

func newZstdDecoder() *zstd.Decoder {
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(16<<20), // 16MB
	)
	if err != nil {
		panic(err)
	}
	return dec
}

type compIO struct {
	buff bytes.Buffer
	mc   *mysqlConn
	comp compressor
}

func newCompIO(mc *mysqlConn) *compIO {
	c := &compIO{mc: mc}
	if mc.cfg.zstdCompress {
		level := zstd.EncoderLevelFromZstd(mc.cfg.zstdCompressionLevel)
		c.comp = &zstdCompressor{level: level}
	} else {
		c.comp = &zlibCompressor{level: mc.cfg.zlibCompressionLevel}
	}
	return c
}

func (c *compIO) close() {
	c.comp.releaseCodecs()
	c.mc = nil
}

const maxIdleBufferSize = 1 << 16 // 64KB

func (c *compIO) reset() {
	// Release large buffers to reduce memory when idle.
	if c.buff.Cap() > maxIdleBufferSize {
		c.buff = bytes.Buffer{}
	} else {
		c.buff.Reset()
	}
	//c.comp.releaseCodecs()
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
	var h compHeader
	h.read(header)
	if debug {
		fmt.Printf("uncompress cmplen=%v uncomplen=%v pkt_cmp_seq=%v expected_cmp_seq=%v\n",
			h.CompressedLength, h.UncompressedLength, h.Sequence, c.mc.compressSequence)
	}
	// Do not return ErrPktSync here.
	// Server may return error packet (e.g. 1153 Got a packet bigger than 'max_allowed_packet' bytes)
	// before receiving all packets from client. In this case, seqnr is younger than expected.
	// NOTE: Both of mariadbclient and mysqlclient do not check seqnr. Only server checks it.
	if debug && h.Sequence != c.mc.compressSequence {
		fmt.Printf("WARN: unexpected compress seq nr: expected %v, got %v",
			c.mc.compressSequence, h.Sequence)
	}
	c.mc.compressSequence = h.Sequence + 1

	comprData, err := c.mc.readNext(int(h.CompressedLength))
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
	c.buff.Grow(int(h.UncompressedLength))
	nread, err := c.comp.uncompress(comprData, &c.buff)
	if err != nil {
		return err
	}
	if nread != int(h.UncompressedLength) {
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
		var header compHeader
		payloadLen := min(maxPayloadLen, len(packets))
		payload := packets[:payloadLen]
		header.UncompressedLength = uint32(payloadLen)

		buf.Reset()
		buf.Write(blankHeader[:]) // Buffer.Write() never returns error

		// If payload is less than minCompressLength, don't compress.
		if header.UncompressedLength < minCompressLength {
			buf.Write(payload)
			header.UncompressedLength = 0
		} else {
			err := c.comp.compress(payload, buf)
			if err != nil {
				c.mc.log("compress error:", err)
			}
			// do not compress if compressed data is larger than uncompressed data.
			// The 7-byte header in buf is intentionally excluded from the comparison;
			// compression must save more than the header overhead to be worthwhile.
			if err != nil || buf.Len() >= int(header.UncompressedLength) {
				buf.Reset()
				buf.Write(blankHeader[:])
				buf.Write(payload)
				header.UncompressedLength = 0
			}
		}
		header.CompressedLength = uint32(buf.Len() - compHeaderSize)
		header.Sequence = c.mc.compressSequence

		if n, err := c.writeCompressedPacket(&header); err != nil {
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
