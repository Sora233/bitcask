package bitcask

import (
	"os"
	"path/filepath"
	"sync"
)

type DB struct {
	indexes map[string]int64
	dbPath  string
	f       *dbFile
	sync.RWMutex
}

func Open(path string) (*DB, error) {
	if s, err := os.Stat(path); os.IsNotExist(err) {
		dir := filepath.Dir(path)
		if err = os.MkdirAll(dir, 0755); err != nil {
			return nil, withErr(err)
		}
	} else if s.IsDir() {
		return nil, ErrPathIsDir
	}
	dbf, err := openDBFile(path)
	if err != nil {
		return nil, err
	}
	db := &DB{
		dbPath: path,
		f:      dbf,
	}
	err = db.rebuildIndexes()
	if err != nil {
		return nil, err
	}
	err = db.Compact()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) Close() error {
	return db.f.Close()
}

func (db *DB) rebuildIndexes() error {
	db.indexes = make(map[string]int64)
	err := db.f.iter(func(entry *Entry, off int64) bool {
		if entry.IsDeleted() {
			delete(db.indexes, string(entry.Key))
		} else {
			db.indexes[string(entry.Key)] = off
		}
		return true
	})
	return err
}

func (db *DB) Put(key, value []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	db.Lock()
	defer db.Unlock()
	e := MakeEntry(key, value)
	off, err := db.f.WriteEntry(e)
	if err != nil {
		return err
	}
	db.indexes[string(key)] = off
	return nil
}

func (db *DB) Exists(key []byte) bool {
	if err := checkKey(key); err != nil {
		return false
	}
	db.Lock()
	defer db.Unlock()
	_, found := db.indexes[string(key)]
	return found
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	if err = checkKey(key); err != nil {
		return
	}
	db.RLock()
	defer db.RUnlock()
	off, found := db.indexes[string(key)]
	if !found {
		return nil, ErrKeyNotFound
	}
	e, err := db.f.ReadEntry(off)
	if err != nil {
		return nil, err
	}
	return e.Value, nil
}

func (db *DB) Delete(key []byte) (err error) {
	if err = checkKey(key); err != nil {
		return ErrKeyTooLarge
	}
	db.Lock()
	defer db.Unlock()
	_, found := db.indexes[string(key)]
	if !found {
		return ErrKeyNotFound
	}
	e := MakeEntry(key, nil)
	e.MarkDeleted()
	_, err = db.f.WriteEntry(e)
	if err != nil {
		return err
	}
	delete(db.indexes, string(key))
	return nil
}

func (db *DB) Compact() error {
	db.Lock()
	defer db.Unlock()

	compactPath := db.dbPath + ".compact"
	cpf, err := openDBFile(compactPath)
	if err != nil {
		return err
	}
	for _, off := range db.indexes {
		e, err := db.f.ReadEntry(off)
		_, err = cpf.WriteEntry(e)
		if err != nil {
			return err
		}
	}
	err = os.Rename(compactPath, db.dbPath)
	if err != nil {
		return withErr(err)
	}

	db.f.Close()
	db.f = cpf
	return db.rebuildIndexes()
}
