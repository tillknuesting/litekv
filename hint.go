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
// that is missing, damaged, half written, or describing a log of a different
// size costs nothing but the time saved. That is also why writing one does not
// wait for the disk.
//
// The trade is that a log covered by a hint is not checked against its
// checksums at startup. A record damaged since it was written is then found by
// the read that wants it, or by Verify, rather than by opening the store.
const (
	hintSuffix = ".hint"
	hintMagic  = "LKVH"

	// hintVersion is 3 because the header gained a byte saying whether the merge
	// that produced this log dropped anything. A hint of an older version is
	// ignored rather than read without it, which costs the scan a hint exists to
	// save — the same bargain every other way of refusing a hint makes.
	hintVersion = 3

	// magic, version, key count, the size of the log it describes, the highest
	// number any record in it carries, and whether records were dropped from it.
	hintHeaderSize = 4 + 1 + 8 + 8 + 8 + 1

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
func writeHint(segmentPath string, segmentSize int64, maxSeq uint64, dropped bool, index map[string]int64) error {
	path := hintPath(segmentPath)
	temp := path + mergeSuffix

	file, err := disk.Open(temp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	failed := func(err error) error {
		file.Close()
		disk.Remove(temp)
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
	binary.LittleEndian.PutUint64(header[21:29], maxSeq)
	if dropped {
		header[29] = 1
	}
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

	// Deliberately not synced. A hint is a cache, and every way an unsynced one
	// can come back wrong is a way of it being ignored: bytes that never
	// reached the disk fail its checksum, and a rename that did not land leaves
	// no hint at all. Both cost a scan of the log and nothing else.
	//
	// Syncing it cost a barrier for every log frozen, which on this machine was
	// four fifths of the time spent writing to a store whose logs rotate: a
	// mean write of 15.5 µs against 3.7 µs, and a worst one of 5.3 ms against
	// 1.0 ms. The data itself is synced according to the policy, as it must be.
	if err := file.Close(); err != nil {
		disk.Remove(temp)
		return err
	}

	if err := disk.Rename(temp, path); err != nil {
		disk.Remove(temp)
		return err
	}
	return nil
}

// loadHint reads the index of the log at segmentPath from its hint, and reports
// whether it could. A false means nothing is wrong beyond having to read the
// log itself: every reason to refuse a hint is a reason to ignore it.
func loadHint(segmentPath string, segmentSize int64) (index map[string]int64, maxSeq uint64, dropped, ok bool) {
	data, err := disk.ReadFile(hintPath(segmentPath))
	if err != nil {
		return nil, 0, false, false
	}
	return parseHint(data, segmentSize)
}

// parseHint reads a hint out of the bytes of one, for a log of segmentSize.
// Every way it can refuse is a way of saying "read the log instead", so it
// checks everything and explains nothing.
func parseHint(data []byte, segmentSize int64) (index map[string]int64, maxSeq uint64, dropped, ok bool) {
	if len(data) < hintHeaderSize+4 {
		return nil, 0, false, false
	}

	if string(data[0:4]) != hintMagic || data[4] != hintVersion {
		return nil, 0, false, false
	}

	body := data[:len(data)-4]
	if binary.LittleEndian.Uint32(data[len(data)-4:]) != crc32.ChecksumIEEE(body) {
		return nil, 0, false, false
	}

	// A hint belongs to the log it was written for. A log of another length has
	// been recovered, replaced, or damaged since, and the offsets in the hint
	// mean nothing for it.
	if int64(binary.LittleEndian.Uint64(data[13:21])) != segmentSize {
		return nil, 0, false, false
	}

	maxSeq = binary.LittleEndian.Uint64(data[21:29])
	dropped = data[29] == 1

	count := binary.LittleEndian.Uint64(data[5:13])
	if count > uint64(len(body)/hintEntrySize) {
		return nil, 0, false, false
	}

	index = make(map[string]int64, count)

	rest := body[hintHeaderSize:]
	for i := uint64(0); i < count; i++ {
		if len(rest) < hintEntrySize {
			return nil, 0, false, false
		}

		pos := int64(binary.LittleEndian.Uint64(rest[0:8]))
		keyLen := binary.LittleEndian.Uint32(rest[8:12])
		rest = rest[hintEntrySize:]

		if uint64(keyLen) > uint64(len(rest)) {
			return nil, 0, false, false
		}
		// An offset that is not in the log would turn a read into an error
		// rather than a value, so a hint claiming one is not to be trusted.
		// The bound is the smallest a record can be, which is the older and
		// shorter of the two layouts.
		if pos < 0 || pos+headerSizeV0 > segmentSize {
			return nil, 0, false, false
		}

		index[string(rest[:keyLen])] = pos
		rest = rest[keyLen:]
	}

	if len(rest) != 0 {
		return nil, 0, false, false
	}

	return index, maxSeq, dropped, true
}

// removeHint deletes the hint for a log, which has to happen before the log it
// describes is replaced: a hint left beside a different log is the one way a
// wrong answer could survive all of this.
func removeHint(segmentPath string) error {
	err := disk.Remove(hintPath(segmentPath))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
