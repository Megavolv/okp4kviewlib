package okp4kviewlib

import (
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

func (l *List) GetKeys(target, count int64) (data string, tail, total int64, err error) {
	k, i := l.FindSuitable(target)

	if target+count >= k.End {
		tail = target + count - k.End
		count = k.End - target
	}
	total = count
	new_target := target - i.Start
	data, err = GetKeysByOneFile(k.f, i.f, new_target, new_target+count)

	return

}

func (l *List) AddKeyFile(path, name string) {
	l.Keys = append(l.Keys, LoadFile(path, name))
}

func (l *List) AddIndexFile(path, name string) {
	l.Indexes = append(l.Indexes, LoadFile(path, name))
}

func (l *List) FindSuitable(target int64) (key *File, index *File) {
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
