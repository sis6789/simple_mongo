package keydb2

import (
	"context"
	"fmt"
	"log"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type bulkBlock struct {
	limit              int
	dbName             string
	collectionName     string
	collection         *mongo.Collection
	chanRequest        chan mongoRequest
	goRoutineRequestWG sync.WaitGroup
	isClosed           bool
	client             *keyDB
	onceClose          sync.Once
}

type mongoRequest struct {
	isFlush bool
	data    mongo.WriteModel
}

// merger - 야러 고루틴에서 보내지는 요구를 모아서 DB에 적용한다.
func goRoutineMerger(b *bulkBlock) {
	defer b.goRoutineRequestWG.Done()
	var tempHolder []mongo.WriteModel
	var wgAsync sync.WaitGroup
	var nonOrderedOpt = options.BulkWrite().SetOrdered(false)
	// define mongo db request
	callMongo := func(models []mongo.WriteModel) {
		defer wgAsync.Done()
		if _, err := b.collection.BulkWrite(context.Background(), models, nonOrderedOpt); err != nil {
			log.Fatalln(b, err)
		}
	}
	// loop for request
	for request := range b.chanRequest {
		if request.isFlush {
			if len(tempHolder) > 0 {
				wgAsync.Add(1)
				go callMongo(tempHolder)
				tempHolder = []mongo.WriteModel{}
			}
		} else {
			tempHolder = append(tempHolder, request.data)
			if len(tempHolder) >= b.limit {
				wgAsync.Add(1)
				go callMongo(tempHolder)
				tempHolder = []mongo.WriteModel{}
			}
		}
	}
	// process remain request
	if len(tempHolder) > 0 {
		wgAsync.Add(1)
		go callMongo(tempHolder)
		tempHolder = []mongo.WriteModel{}
	}
	wgAsync.Wait()
	b.isClosed = true
}

// NewBulk - prepare bulk operation
func (x *keyDB) NewBulk(dbName, collectionName string, interval int) *bulkBlock {
	dbCol := dbName + "::" + collectionName
	initializeBlock := func(pB *bulkBlock) {
		pB.client = x
		pB.isClosed = false
		pB.dbName = dbName
		pB.collectionName = collectionName
		pB.collection = x.Col(dbName, collectionName)
		pB.limit = interval
		pB.chanRequest = make(chan mongoRequest)
		pB.onceClose = sync.Once{}
		pB.goRoutineRequestWG.Add(1)
		go goRoutineMerger(pB)
	}
	iVal, exist := x.mapBulk.Load(dbCol)
	if exist {
		if iVal.(*bulkBlock).isClosed {
			pB := iVal.(*bulkBlock)
			// restart channel
			initializeBlock(pB)
			return pB
		} else {
			return iVal.(*bulkBlock)
		}
	} else {
		// create new chaneel
		var b bulkBlock
		pB := &b
		initializeBlock(pB)
		log.Printf("bulk start: %v", pB)
		return pB
	}
}

// InsertOne - append action InsertOne.
func (bb *bulkBlock) InsertOne(model *mongo.InsertOneModel) {
	if bb.isClosed {
		log.Fatalf("put after close: mongo:%v db:%v col:%v", bb.client.mongodbAccess, bb.dbName, bb.collectionName)
	} else {
		bb.chanRequest <- mongoRequest{
			isFlush: false,
			data:    model,
		}
	}
}

// UpdateOne - append action UpdateOne.
func (bb *bulkBlock) UpdateOne(model *mongo.UpdateOneModel) {
	if bb.isClosed {
		log.Fatalf("put after close: mongo:%v db:%v col:%v", bb.client.mongodbAccess, bb.dbName, bb.collectionName)
	} else {
		bb.chanRequest <- mongoRequest{
			isFlush: false,
			data:    model,
		}
	}
}

func (bb *bulkBlock) Flush() {
	bb.chanRequest <- mongoRequest{
		isFlush: true,
		data:    nil,
	}
}

// Close - send remain accumulated request.
func (bb *bulkBlock) Close() {
	close(bb.chanRequest)
	log.Printf("bulk close: %v", bb)
}

// String - status message
func (bb *bulkBlock) String() string {
	return fmt.Sprintf("mongo:%v db:%v col:%v", bb.client.mongodbAccess, bb.dbName, bb.collectionName)
}
