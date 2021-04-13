package index

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"math/rand"
	"os"
	"sort"
)

type IndexItem struct {
	partialKey string
	offset     int64
	size       int64
}

func NewIndexItem(key string, offset int64, size int64) IndexItem {
	pk := getPartialKey(key)
	return IndexItem{pk, offset, size}
}

func (i *IndexItem) Size() int64 {
	return i.size
}

func (i *IndexItem) PartialKey() string {
	return i.partialKey
}

func (i *IndexItem) Offset() int64 {
	return i.offset
}

type Index interface {
	Get(key string) (indexItems []IndexItem, ok bool)
	Put(indexItem IndexItem)
	Del(key string)
	DataLog() DataLog
	Save() error
	Load() error
}

func getPartialKey(key string) string {
	partialKey := key
	if len(key) > 16 {
		partialKey = key[0:15]
	}

	return partialKey
}

type LocalIndex struct {
	storageFilePath string
	indexItems      map[string][]IndexItem
	localDataLog    DataLog
}

func (i *LocalIndex) DataLog() DataLog {
	return i.localDataLog
}

func (i *LocalIndex) Save() error {
	var items []IndexItem
	iitems := i.indexItems
	log.Infof("Saving index file to %s", i.storageFilePath)
	log.Infof("Collecting index items from index map of size %d.", len(iitems))
	for _, value := range iitems {
		for _, it := range value {
			items = append(items, it)
		}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Offset() < items[j].Offset()
	})
	log.Info("Sorted index items by offset.")

	log.Info("Creating temp index file.")
	fileName := fmt.Sprintf("./index_swap%d.csv", rand.Int())
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Error("Could not create tmp index file.", err)
		return err
	}
	defer file.Close()

	log.Infof("Writing %d records to index.", len(items))
	for _, item := range items {
		_, write_err := file.WriteString(fmt.Sprintf("%s,%d,%d\n", item.PartialKey(), item.Offset(), item.Size()))
		if write_err != nil {
			return write_err
		}
	}

	//Create index file if no exist
	log.Infof("Swapping tmp index file as replacement.")
	err = os.Rename(fileName, i.storageFilePath)

	return err
}

func (i *LocalIndex) Get(key string) (indexItems []IndexItem, ok bool) {
	partialKey := getPartialKey(key)
	indexItems, ok = i.indexItems[partialKey]
	return indexItems, ok
}

func (i *LocalIndex) Put(indexItem IndexItem) {
	log.Infof("Adding index item for partial key %s.", indexItem.PartialKey())
	indexItems, ok := i.indexItems[indexItem.PartialKey()]
	if !ok {
		log.Infof("New key entry for %s, making new list.", indexItem.PartialKey())
		indexItems = make([]IndexItem, 0)
	}

	indexItems = append(indexItems, indexItem)
	i.indexItems[indexItem.PartialKey()] = indexItems
	log.Infof("Added index item for partial key %s.", indexItem.PartialKey())
}

func (i *LocalIndex) getLastIndex() int64 {
	storeFile, err := os.OpenFile(i.storageFilePath, os.O_RDONLY, 0644)

	if _, er := os.Stat(i.storageFilePath); os.IsNotExist(er) {
		return 0
	}

	if err != nil {
		log.Errorf("Unable to open index file at %s.", i.storageFilePath)
		return 0
	}

	defer storeFile.Close()

	stat, err := storeFile.Stat()
	if err != nil {
		log.Errorf("Unable to seek to offset in index file at %s", i.storageFilePath, err)
		return 0
	}

	return stat.Size()
}

func (i *LocalIndex) Del(key string) {
	log.Infof("Deleting index item for key %s", key)
	indexItems, ok := i.Get(key)

	if !ok {
		log.Infof("Index item for key %s does not exist, delete redundant.", key)
		return
	}

	for index, item := range indexItems {
		log.Infof("Look %d", item.Offset())
		logItem, err := i.localDataLog.ReadLogItem(item.Offset())
		if err != nil {
			break
		}

		if logItem.Key() == key {
			log.Infof("Log item found deleting for %s.", key)
			indexItems[index] = indexItems[len(indexItems)-1]
			i.indexItems[getPartialKey(key)] = indexItems[:len(indexItems)-1]
			continue
		}
	}
}

func (i *LocalIndex) Load() error {
	log.Infof("Loading index data from %s", i.storageFilePath)
	indexItems := i.indexItems
	dataLog := i.localDataLog

	var offset int64 = i.getLastIndex()
	log.Infof("Last index is %d", offset)
	for true {
		logItem, err := dataLog.ReadLogItem(offset)
		if err == io.EOF {
			log.Infof("Reached end of data log file.")
			break
		}

		if err != nil {
			log.Error("Error encountered during reading log item.", err)
			return err
		}

		item := NewIndexItem(logItem.Key(), logItem.Offset(), logItem.Size())
		indexItems[item.PartialKey()] = append(indexItems[item.PartialKey()], item)
		offset = item.offset + item.size
	}

	i.indexItems = indexItems

	log.Infof("Loaded index data from %s", i.storageFilePath)
	return nil
}

func NewLocalIndex(storageFilePath string, dataLog DataLog) Index {
	indexItems := make(map[string][]IndexItem)
	localIndex := LocalIndex{storageFilePath, indexItems, dataLog}

	return &localIndex
}
