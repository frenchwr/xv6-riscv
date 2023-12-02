package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"path"
	"unsafe"
)

const (
	rootIno      = 1
	bSize        = 1024
	fsSize       = 2000
	fsMagic      = 0x10203040
	nDirect      = 12
	nIndirect    = bSize / int(unsafe.Sizeof(freeinode))
	maxFile      = nDirect + nIndirect
	ipb          = bSize / int(unsafe.Sizeof(dInode{})) // Inodes per block
	bpb          = bSize * 8                            // bitmap bits per block
	dirSiz       = 14
	nInodes      = 200
	nBitMap      = fsSize/(bSize*8) + 1
	nInodeBlocks = nInodes/ipb + 1
	maxOpBlocks  = 10 // max # of blocks any FS op writes
	nLog         = maxOpBlocks * 3
	nMeta        = 2 + nLog + nInodeBlocks + nBitMap
	nBlocks      = fsSize - nMeta
	tDir         = 1
	tFile        = 2
	tDevice      = 3
)

type superblock struct {
	Magic      uint32
	Size       uint32
	Nblocks    uint32
	Ninodes    uint32
	Nlog       uint32
	LogStart   uint32
	InodeStart uint32
	BmapStart  uint32
}

type dInode struct {
	NodeType int16
	Major    int16
	Minor    int16
	Nlink    int16
	Size     uint32
	Addrs    [nDirect + 1]uint32
}

type dirent struct {
	inum uint16
	name [dirSiz]byte
}

var fsfd *os.File
var freeinode uint32 = 1
var sb superblock
var freeBlock uint32

func main() {

	var err error
	fsfd, err = os.OpenFile(os.Args[1], os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}

	sb = superblock{
		Magic:      fsMagic,
		Size:       fsSize,
		Nblocks:    uint32(nBlocks),
		Ninodes:    nInodes,
		Nlog:       nLog,
		LogStart:   2,
		InodeStart: 2 + nLog,
		BmapStart:  uint32(2 + nLog + nInodeBlocks),
	}

	log.Printf("nmeta %d (boot, super, log blocks %d inode blocks %d, bitmap blocks %d) blocks %d total %d\n",
		nMeta, nLog, nInodeBlocks, nBitMap, nBlocks, fsSize)

	freeBlock = uint32(nMeta) // the first free block that we can allocate

	// fill image file with zeros
	zeroes := make([]byte, bSize)
	for i := uint32(0); i < uint32(fsSize); i++ {
		wsect(i, zeroes)
	}

	// write superblock to first sector/block
	buf := make([]byte, bSize)
	copy(buf, structToBytes(sb))
	wsect(1, buf)

	// allocate root innode and write it to image file
	rootino := ialloc(tDir)
	var rootDirName [14]byte
	copy(rootDirName[:], ".")
	de := dirent{
		inum: uint16(rootino),
		name: rootDirName,
	}
	iappend(rootino, structToBytes(de), int(unsafe.Sizeof(de)))
	copy(de.name[:], "..")
	iappend(rootino, structToBytes(de), int(unsafe.Sizeof(de)))

	// write user space binaries to image file
	for _, arg := range os.Args[2:] {
		shortName := path.Base(arg)
		if shortName[0] == '_' {
			shortName = shortName[1:]
		}
		fd, err := os.Open(arg)
		if err != nil {
			log.Fatal(err)
		}
		inum := ialloc(tFile)
		var fileName [14]byte
		copy(fileName[:], shortName)
		de := dirent{
			inum: uint16(inum),
			name: fileName,
		}
		iappend(rootino, structToBytes(de), int(unsafe.Sizeof(de)))
		cc, err := fd.Read(buf)
		if err != nil {
			log.Fatal(err)
		}
		for cc > 0 {
			iappend(inum, buf, cc)
			cc, err = fd.Read(buf)
		}
		fd.Close()
	}

	// fix size of root inode dir
	var din dInode
	rinode(rootino, &din)
	off := ((din.Size / bSize) + 1) * bSize
	din.Size = off
	winode(rootino, &din)

	// write block usage bitmap
	balloc(int(freeBlock))

	fsfd.Close()

}

// write free block bitmap
func balloc(used int) {
	buf := make([]byte, bSize)
	log.Printf("balloc: first %d blocks have been allocated\n", used)
	if used >= bSize*8 {
		log.Fatal("Too many blocks in use.")
	}
	for i := 0; i < used; i++ {
		buf[i/8] = buf[i/8] | (0x1 << (i % 8))
	}
	log.Printf("balloc: write bitmap block at sector %d\n", sb.BmapStart)
	wsect(sb.BmapStart, buf)
}

// write data p (with size n bytes) for underlying inode inum
//
// this is invoked in a few ways:
// (i) call on directory where data is a dirent struct casted as an array of bytes.
// (ii) call on data being read from file by caller in chunks of size n.
func iappend(inum uint32, data []byte, nBytes int) {
	var din dInode
	var blockAddr uint32

	rinode(inum, &din)
	dinSize := din.Size
	//log.Printf("append inum %d at dinSize %d sz %d\n", inum, dinSize, nBytes)
	for nBytes > 0 {
		fbn := dinSize / bSize // number of blocks currently in use by file referenced in inode
		if fbn >= uint32(maxFile) {
			log.Fatal("Max file size exceeded.")
		}

		// store block addresses directly in inode if slots available in Addrs array
		if fbn < nDirect {
			if din.Addrs[fbn] == 0 {
				din.Addrs[fbn] = freeBlock
				freeBlock++
			}
			blockAddr = din.Addrs[fbn]
		} else { // Addrs array in inode exhausted, start storing block addresses in their own block
			if din.Addrs[nDirect] == 0 {
				din.Addrs[nDirect] = freeBlock
				freeBlock++
			}
			indirectAddrs := make([]byte, bSize)
			rsect(din.Addrs[nDirect], indirectAddrs)
			nIndirectAddrs := fbn - nDirect // number of addresses currently stored in indirect block

			nBytesPerUint32 := uint32(unsafe.Sizeof(blockAddr))
			addrBytes := indirectAddrs[nIndirectAddrs*nBytesPerUint32 : (nIndirectAddrs+1)*nBytesPerUint32]
			addrUint32 := binary.LittleEndian.Uint32(addrBytes)
			if addrUint32 == 0 {
				binary.LittleEndian.PutUint32(addrBytes, freeBlock)
				freeBlock++
				wsect(din.Addrs[nDirect], indirectAddrs)
			}
			blockAddr = binary.LittleEndian.Uint32(addrBytes)
		}
		nBytesCorrected := min(nBytes, int((fbn+1)*bSize-dinSize))
		buf := make([]byte, bSize)
		rsect(blockAddr, buf)
		startOffset := dinSize - (fbn * bSize)
		copy(buf[startOffset:], data[:nBytesCorrected])
		wsect(blockAddr, buf)
		nBytes -= nBytesCorrected
		dinSize += uint32(nBytesCorrected)
		data = data[nBytesCorrected:]
	}
	din.Size = dinSize
	winode(inum, &din)
}

// return block containing inode i
func iblock(i uint32, iNodeStart uint32) uint32 {
	return i/uint32(ipb) + iNodeStart
}

// return block of free map containing bit for block b
func bblock(b int, bMapStart int) int {
	return b/bpb + bMapStart
}

// convert generic struct to array of bytes
func structToBytes(i interface{}) []byte {
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.LittleEndian, i)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

// allocate new inode and write it to disk
func ialloc(nodeType uint16) uint32 {
	var inum uint32 = freeinode
	freeinode++
	din := dInode{
		NodeType: int16(nodeType),
		Nlink:    1,
	}
	winode(inum, &din)
	return inum
}

// write bytes to specified sector. note that each sector only
// contains a single block so you can think of the sector as the block.
func wsect(sect uint32, buf []byte) {
	targetOffset := int64(sect * uint32(bSize))
	// note that second arg means we are seeking from the beginning of the file
	offset, err := fsfd.Seek(targetOffset, 0)
	if err != nil {
		log.Fatal(err)
	}
	if offset != targetOffset {
		log.Fatal("offset and targetOffset do not match!")
	}
	// note that this increments fsfd offset by len(buf)
	bytesWriten, err := fsfd.Write(buf)
	if err != nil {
		log.Fatal(err)
	}
	if bytesWriten != bSize {
		log.Fatal("Should only be writing bSize bytes!")
	}
}

// write innode to disk
func winode(inum uint32, ip *dInode) {
	buf := make([]byte, bSize)
	bn := iblock(inum, sb.InodeStart)
	// read block into buffer, then write new innode
	// into buffer, and write the buffer back to the same block
	rsect(bn, buf)
	inodeSize := int(unsafe.Sizeof(dInode{}))
	writeOffset := inodeSize * (int(inum) % ipb)
	copy(buf[writeOffset:], structToBytes(*ip))
	wsect(bn, buf)
}

// read innode and store into ip param
func rinode(inum uint32, ip *dInode) {
	buf := make([]byte, bSize)
	bn := iblock(inum, sb.InodeStart)
	// read block into buffer, then write new innode
	// into buffer, and write the buffer back to the same block
	rsect(bn, buf)
	inodeSize := int(unsafe.Sizeof(dInode{}))
	readOffsetStart := inodeSize * (int(inum) % ipb)
	readOffsetEnd := readOffsetStart + inodeSize
	// get slice of buffer corresponding to innode we're reading
	inodeBuffer := buf[readOffsetStart:readOffsetEnd]
	err := binary.Read(bytes.NewReader(inodeBuffer), binary.LittleEndian, ip)
	if err != nil {
		log.Fatal(err)
	}
}

// read bytes from specified sector/block.
func rsect(sect uint32, buf []byte) {
	targetOffset := int64(sect * uint32(bSize))
	offset, err := fsfd.Seek(targetOffset, 0)
	if err != nil {
		log.Fatal(err)
	}
	if offset != targetOffset {
		log.Fatal("offset and targetOffset do not match!")
	}
	// note that this increments fsfd offset by len(buf)
	bytesRead, err := fsfd.Read(buf)
	if err != nil {
		log.Fatal(err)
	}
	if bytesRead != bSize {
		log.Fatal("Should only be reading bSize bytes!")
	}
}
