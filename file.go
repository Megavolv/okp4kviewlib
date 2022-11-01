package okp4kviewlib

import (
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

type File struct {
	Name       string
	Start, End int64
	f          *os.File
}

type FileMan struct {
	logger *logrus.Logger
}

func NewFileMan(logger *logrus.Logger) *FileMan {
	return &FileMan{logger: logger}
}

func (man FileMan) GetKeysByOneFile(fkey, findex *os.File, start, end int64) (string, error) {
	a, err := man.GetKeyPosition(findex, start)
	if err != nil {
		man.logger.Error(err)
		return "", err
	}

	man.logger.Debug("Will start reading at: ", a)

	b, err := man.GetKeyPosition(findex, end)
	if err != nil {
		info, _ := fkey.Stat()
		b = uint64(info.Size())
		man.logger.Debug("Will finish reading at the end of the file")
	} else {
		man.logger.Debug("Will finish reading at: ", b)
	}

	_, err = fkey.Seek(int64(a), 0)
	if err != nil {
		panic(err)
	}

	kbuf := make([]byte, b-a-1)

	man.logger.Debug("Key buffer len is ", b-a-1)

	_, err = fkey.Read(kbuf)
	if err != nil {
		panic(err)
	}

	return string(kbuf), nil
}

func (man FileMan) GetKeyPosition(findex *os.File, position int64) (key_offset uint64, err error) {
	man.logger.Debug("GetKeyPosition. Position:", position)
	man.logger.Debug("GetKeyPosition. Seek to:", position*8)

	_, err = findex.Seek(position*8, 0)
	if err != nil {
		man.logger.Error("GetKeyPosition. Seek err: ", err)
		return 0, err
	}

	buf := make([]byte, 8)

	_, err = findex.Read(buf)
	if err != nil {
		man.logger.Error("GetKeyPosition. Read err: ", err)
		return 0, err
	}

	man.logger.Debug("GetKeyPosition: buf is ", hex.EncodeToString(buf))

	key_offset = binary.LittleEndian.Uint64(buf[0:8])

	man.logger.Debug("GetKeyPosition: buf uint64 is ", key_offset)

	return
}

func (man FileMan) LoadFile(path, name string) *File {
	data := strings.Split(name, ".")

	start_end := strings.Split(data[1], "-")
	start, err := strconv.ParseInt(start_end[0], 10, 64)

	if err != nil {
		panic(err)
	}

	end, err := strconv.ParseInt(start_end[1], 10, 64)
	if err != nil {
		panic(err)
	}

	f := &File{
		Name:  name,
		Start: start,
		End:   end,
	}

	f.f, err = os.OpenFile(filepath.Join(path, name), os.O_RDONLY, 0755)
	if err != nil {
		panic(err)
	}

	return f
}
