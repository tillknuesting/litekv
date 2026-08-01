package litekv

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"strings"
)

// A hint file is the index of a frozen log, written down beside it.
//
// Without one, opening a store means reading every byte of every log and
// checking every record, because that is the only way to learn where the keys
// are. That is work proportional to what is stored rather than to how many keys
// there are, and on slow storage it is most of the time it takes to start.
//
// A hint holds a key and an offset per record, so reading one costs about
// twenty bytes per key instead of the whole log. It is only ever a shortcut:
// anything wrong with it and the log is read the long way instead, so a hint
// that is missing, damaged, or describing a log of a different size costs
// nothing but the time saved.
//
// The trade is that a log covered by a hint is not checked against its
// checksums at startup. A record damaged since it was written is then found by
// the read that wants it, or by Verify, rather than by opening the store.
const (
	hintSuffix  = ".hint"
	hintMagic   = "LKVH"
	hintVersion = 1

	// magic, version, key count, and the size of the log it describes.
	hintHeaderSize = 4 + 1 + 8 + 8

	// offset and key length, per entry.
	hintEntrySize = 8 + 4
)

// hintPath is where the hint for a log lives.
func hintPath(segmentPath string) string {
	return strings.TrimSuffix(segmentPath, segmentSuffix) + hintSuffix
}

// writeHint records index beside the log at segmentPath, which is segmentSize
// bytes long. It is written to one side and renamed into place, so a hint that
// exists is a hint that was finished.
func writeHint(segmentPath string, segmentSize int64, index map[string]int64) error {
	path := hintPath(segmentPath)
	temp := path + mergeSuffix

	file, err := os.OpenFile(temp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	failed := func(err error) error {
		file.Close()
		os.Remove(temp)
		return err
	}

	writer := bufio.NewWriterSize(file, 64<<10)
	sum := crc32.NewIEEE()
	both := io.MultiWriter(writer, sum)

	var header [hintHeaderSize]byte
	copy(header[0:4], hintMagic)
	header[4] = hintVersion
	binary.LittleEndian.PutUint64(header[5:13], uint64(len(index)))
	binary.LittleEndian.PutUint64(header[13:21], uint64(segmentSize))
	if _, err := both.Write(header[:]); err != nil {
		return failed(err)
	}

	var entry [hintEntrySize]byte
	for key, pos := range index {
		binary.LittleEndian.PutUint64(entry[0:8], uint64(pos))
		binary.LittleEndian.PutUint32(entry[8:12], uint32(len(key)))
		if _, err := both.Write(entry[:]); err != nil {
			return failed(err)
		}
		if _, err := io.WriteString(both, key); err != nil {
			return failed(err)
		}
	}

	// The checksum covers everything before it, and so is not part of itself.
	var trailer [4]byte
	binary.LittleEndian.PutUint32(trailer[:], sum.Sum32())
	if _, err := writer.Write(trailer[:]); err != nil {
		return failed(err)
	}

	if err := writer.Flush(); err != nil {
		return failed(err)
	}
	if err := file.Sync(); err != nil {
		return failed(err)
	}
	if err := file.Close(); err != nil {
		os.Remove(temp)
		return err
	}

	if err := os.Rename(temp, path); err != nil {
		os.Remove(temp)
		return err
	}
	return nil
}

// loadHint reads the index of the log at segmentPath from its hint, and reports
// whether it could. A false means nothing is wrong beyond having to read the
// log itself: every reason to refuse a hint is a reason to ignore it.
func loadHint(segmentPath string, segmentSize int64) (map[string]int64, bool) {
	data, err := os.ReadFile(hintPath(segmentPath))
	if err != nil || len(data) < hintHeaderSize+4 {
		return nil, false
	}

	if string(data[0:4]) != hintMagic || data[4] != hintVersion {
		return nil, false
	}

	body := data[:len(data)-4]
	if binary.LittleEndian.Uint32(data[len(data)-4:]) != crc32.ChecksumIEEE(body) {
		return nil, false
	}

	// A hint belongs to the log it was written for. A log of another length has
	// been recovered, replaced, or damaged since, and the offsets in the hint
	// mean nothing for it.
	if int64(binary.LittleEndian.Uint64(data[13:21])) != segmentSize {
		return nil, false
	}

	count := binary.LittleEndian.Uint64(data[5:13])
	if count > uint64(len(body)/hintEntrySize) {
		return nil, false
	}

	index := make(map[string]int64, count)

	rest := body[hintHeaderSize:]
	for i := uint64(0); i < count; i++ {
		if len(rest) < hintEntrySize {
			return nil, false
		}

		pos := int64(binary.LittleEndian.Uint64(rest[0:8]))
		keyLen := binary.LittleEndian.Uint32(rest[8:12])
		rest = rest[hintEntrySize:]

		if uint64(keyLen) > uint64(len(rest)) {
			return nil, false
		}
		// An offset that is not in the log would turn a read into an error
		// rather than a value, so a hint claiming one is not to be trusted.
		// The bound is the smallest a record can be, which is the older and
		// shorter of the two layouts.
		if pos < 0 || pos+headerSizeV0 > segmentSize {
			return nil, false
		}

		index[string(rest[:keyLen])] = pos
		rest = rest[keyLen:]
	}

	if len(rest) != 0 {
		return nil, false
	}

	return index, true
}

// removeHint deletes the hint for a log, which has to happen before the log it
// describes is replaced: a hint left beside a different log is the one way a
// wrong answer could survive all of this.
func removeHint(segmentPath string) error {
	err := os.Remove(hintPath(segmentPath))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
