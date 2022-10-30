package okp4kviewlib

import (
	"errors"
	"io/ioutil"
	"path/filepath"
)

type List struct {
	Keys    []*File
	Indexes []*File
}

func NewList(path string) *List {
	list := &List{
		Keys:    []*File{},
		Indexes: []*File{},
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

			list.AddIndexFile(path, file.Name())
		case ".json":

			list.AddKeyFile(path, file.Name())
		}
	}

	return list

}

func (l *List) GetKeys(target, count int64) (data string, total int64, err error) {
	var next_data string
	var k, i *File

	for count > 0 {
		k, i, err = l.FindSuitable(target)
		if err != nil {
			break
		}

		if target+count >= k.End {
			count = k.End - target
		}

		total = count

		new_target := target - i.Start
		next_data, err = GetKeysByOneFile(k.f, i.f, new_target, new_target+count)

		// данные не найдены
		if err != nil {
			break
		}

		// Получено меньше, чем планировали
		if total == count {
			break
		}

		data += next_data

		target = target + total
		count = count - total
	}

	return
}

func (l *List) AddKeyFile(path, name string) {
	l.Keys = append(l.Keys, LoadFile(path, name))
}

func (l *List) AddIndexFile(path, name string) {
	l.Indexes = append(l.Indexes, LoadFile(path, name))
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
	}

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
