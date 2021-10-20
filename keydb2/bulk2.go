package keydb2

import (
	"context"
	"fmt"
	"log"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BulkBlock struct {
	limit              int
	dbName             string
	collectionName     string
	collection         *mongo.Collection
	goRoutineRequest   chan mongo.WriteModel
	goRoutineFlush     chan bool
	goRoutineRequestWG sync.WaitGroup
	isClosed           bool
	client             *KeyDB
	lockClose          sync.Mutex
}

// merger - 야러 고루틴에서 보내지는 요구를 모아서 DB에 적용한다.
func goRoutineMerger(b *BulkBlock) {
	defer b.goRoutineRequestWG.Done()
	var tempHolder []mongo.WriteModel
	var nonOrderedOpt = options.BulkWrite().SetOrdered(false)
	var wgAsync sync.WaitGroup
	for request := range b.goRoutineRequest {
		tempHolder = append(tempHolder, request)
		// check flush request
		select {
		case <-b.goRoutineFlush:
			wgAsync.Add(1)
			go func(models []mongo.WriteModel) {
				defer wgAsync.Done()
				if _, err := b.collection.BulkWrite(context.Background(), models, nonOrderedOpt); err != nil {
					log.Fatalln(b, err)
				}
			}(tempHolder) // call by value로
			tempHolder = []mongo.WriteModel{}
		default:
		}
		if len(tempHolder) >= b.limit {
			wgAsync.Add(1)
			go func(models []mongo.WriteModel) {
				defer wgAsync.Done()
				if _, err := b.collection.BulkWrite(context.Background(), models, nonOrderedOpt); err != nil {
					log.Fatalln(b, err)
				}
			}(tempHolder)
			tempHolder = []mongo.WriteModel{}
		}
	}
	// sender close channel
	if len(tempHolder) > 0 {
		wgAsync.Add(1)
		go func(models []mongo.WriteModel) {
			defer wgAsync.Done()
			if _, err := b.collection.BulkWrite(context.Background(), models, nonOrderedOpt); err != nil {
				log.Fatalln(b, err)
			}
		}(tempHolder)
		tempHolder = []mongo.WriteModel{}
	}
	wgAsync.Wait()
	log.Printf("close bulk: mongo:%v db:%v col:%v", b.client.mongodbAccess, b.dbName, b.collectionName)
}

// NewBulk - prepare bulk operation
func (x *KeyDB) NewBulk(dbName, collectionName string, interval int) *BulkBlock {
	dbCol := dbName + "::" + collectionName
	iVal, exist := x.mapBulk.Load(dbCol)
	if exist {
		if iVal.(*BulkBlock).isClosed {
			pB := iVal.(*BulkBlock)
			// restart channel
			pB.isClosed = false
			pB.dbName = dbName
			pB.collectionName = collectionName
			pB.collection = x.Col(dbName, collectionName)
			pB.limit = interval
			pB.goRoutineRequest = make(chan mongo.WriteModel)
			pB.goRoutineFlush = make(chan bool)
			pB.goRoutineRequestWG.Add(1)
			go goRoutineMerger(pB)
			return pB
		} else {
			return iVal.(*BulkBlock)
		}
	} else {
		// create new chaneel
		var b BulkBlock
		pB := &b
		b.client = x
		b.isClosed = false
		b.dbName = dbName
		b.collectionName = collectionName
		b.collection = x.Col(dbName, collectionName)
		b.limit = interval
		b.goRoutineRequest = make(chan mongo.WriteModel)
		b.goRoutineRequestWG.Add(1)
		x.mapBulk.Store(dbCol, &b)
		go goRoutineMerger(pB)
		log.Printf("newBulk start: mongo:%v db:%v col:%v", x.mongodbAccess, dbName, collectionName)
		return pB
	}
}

// InsertOne - append action InsertOne.
func (bb *BulkBlock) InsertOne(model *mongo.InsertOneModel) {
	if bb.isClosed {
		log.Fatalf("put after close: mongo:%v db:%v col:%v", bb.client.mongodbAccess, bb.dbName, bb.collectionName)
	} else {
		bb.goRoutineRequest <- model
	}
}

// UpdateOne - append action UpdateOne.
func (bb *BulkBlock) UpdateOne(model *mongo.UpdateOneModel) {
	if bb.isClosed {
		log.Fatalf("put after close: mongo:%v db:%v col:%v", bb.client.mongodbAccess, bb.dbName, bb.collectionName)
	} else {
		bb.goRoutineRequest <- model
	}
}

func (bb *BulkBlock) Flush() {
	bb.goRoutineFlush <- true
}

// Close - send remain accumulated request.
func (bb *BulkBlock) Close() {
	if bb.isClosed {
		return
	}
	bb.lockClose.Lock()
	bb.isClosed = true
	close(bb.goRoutineFlush)
	close(bb.goRoutineRequest)
	bb.goRoutineRequestWG.Wait()
	bb.isClosed = true
	bb.goRoutineRequest = nil
	bb.lockClose.Unlock()
}

// String - status message
func (bb *BulkBlock) String() string {
	return fmt.Sprintf("mongo:%v db:%v col:%v", bb.client.mongodbAccess, bb.dbName, bb.collectionName)
}
