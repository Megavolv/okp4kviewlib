package okp4kviewlib

import (
	"errors"
	"io/ioutil"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

type List struct {
	Keys    []*File
	Indexes []*File
	logger  *logrus.Logger
	fileMan *FileMan
}

func NewList(path string, logger *logrus.Logger) *List {
	list := &List{
		Keys:    []*File{},
		Indexes: []*File{},
		logger:  logger,
		fileMan: NewFileMan(logger),
	}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		panic(err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		switch filepath.Ext(file.Name()) {
		case ".idx":
			list.Indexes = append(list.Indexes, list.fileMan.LoadFile(path, file.Name()))
		case ".json":
			list.Keys = append(list.Keys, list.fileMan.LoadFile(path, file.Name()))
		}
	}

	return list

}

// Умеет получать данные с пересечением границы файлов
func (l *List) GetKeys(target, count int64) (data string, err error) {
	var tdata string
	var total int64

	for count > 0 {
		tdata, total, err = l.GetLimitedKeys(target, count)
		if err != nil { // данные не найдены
			break
		}
		data += tdata

		// Получено меньше, чем планировали
		if total == count {
			break
		}

		target = target + total
		count = count - total
	}

	return
}

// GetLimitedKeys возвращает ключи из одного файла
func (l *List) GetLimitedKeys(target, count int64) (data string, total int64, err error) {
	l.logger.Debug("GetLimitedKeys. Target: ", target)
	l.logger.Debug("GetLimitedKeys. Count: ", count)

	k, i, err := l.FindSuitable(target)
	if err != nil {
		return
	}

	if target+count >= k.End {
		count = k.End - target
	}

	total = count

	new_target := target - i.Start

	data, err = l.fileMan.GetKeysByOneFile(k.f, i.f, new_target, new_target+count)

	return
}

func (l *List) FindSuitable(target int64) (key *File, index *File, err error) {
	key = nil
	index = nil
	for _, f := range l.Keys {
		if target >= f.Start && target < f.End {
			key = f
		}
	}

	for _, f := range l.Indexes {
		if target >= f.Start && target < f.End {
			index = f
		}
	}

	if key == nil || index == nil {
		err = errors.New("Index out of range")
		l.logger.Error("FindSuitable. Index out of range")
		return
	}

	l.logger.Debug("FindSuitable. Use keys file: ", key)
	l.logger.Debug("FindSuitable. Use index file: ", index)

	return
}

func (l *List) CloseAll() {
	for _, c := range l.Keys {
		c.f.Close()
	}
	for _, c := range l.Indexes {
		c.f.Close()
	}
}
