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
	chanRequest        chan mongoRequest
	goRoutineRequestWG sync.WaitGroup
	isClosed           bool
	client             *keyDB
	onceClose          sync.Once
}

type mongoRequest struct {
	isFlush bool
	isClose bool
	data    mongo.WriteModel
}

// merger - 야러 고루틴에서 보내지는 요구를 모아서 DB에 적용한다.
func goRoutineMerger(b *BulkBlock) {
	defer b.goRoutineRequestWG.Done()
	var tempHolder []mongo.WriteModel
	var isDoneAllBulkWrite sync.WaitGroup
	var nonOrderedOpt = options.BulkWrite().SetOrdered(false)
	// define mongo db request
	callMongo := func(models []mongo.WriteModel) {
		defer isDoneAllBulkWrite.Done()
		if _, err := b.collection.BulkWrite(context.Background(), models, nonOrderedOpt); err != nil {
			log.Println(b, err)
		} else {
			log.Printf("done %v %v", b, len(models))
		}
	}
	// loop for request
	for request := range b.chanRequest {
		if request.isFlush {
			if len(tempHolder) > 0 {
				isDoneAllBulkWrite.Add(1)
				go callMongo(tempHolder)
				tempHolder = []mongo.WriteModel{}
			}
		} else if request.isClose {
			break
		} else {
			tempHolder = append(tempHolder, request.data)
			if len(tempHolder) >= b.limit {
				isDoneAllBulkWrite.Add(1)
				go callMongo(tempHolder)
				tempHolder = []mongo.WriteModel{}
			}
		}
	}
	// process remain request
	if len(tempHolder) > 0 {
		isDoneAllBulkWrite.Add(1)
		go callMongo(tempHolder)
		tempHolder = []mongo.WriteModel{}
	}
	close(b.chanRequest)
	isDoneAllBulkWrite.Wait()
	b.isClosed = true
	log.Printf("bulk close: %v", b)
}

// NewBulk - prepare bulk operation
func (x *keyDB) NewBulk(dbName, collectionName string, interval int) *BulkBlock {
	dbCol := dbName + "::" + collectionName
	initializeBlock := func(pB *BulkBlock) {
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
		if iVal.(*BulkBlock).isClosed {
			pB := iVal.(*BulkBlock)
			// restart channel
			initializeBlock(pB)
			return pB
		} else {
			return iVal.(*BulkBlock)
		}
	} else {
		// create new chaneel
		var b BulkBlock
		pB := &b
		initializeBlock(pB)
		log.Printf("bulk start: %v", pB)
		return pB
	}
}

// InsertOne - append action InsertOne.
func (bb *BulkBlock) InsertOne(model *mongo.InsertOneModel) {
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
func (bb *BulkBlock) UpdateOne(model *mongo.UpdateOneModel) {
	if bb.isClosed {
		log.Fatalf("put after close: mongo:%v db:%v col:%v", bb.client.mongodbAccess, bb.dbName, bb.collectionName)
	} else {
		bb.chanRequest <- mongoRequest{
			isFlush: false,
			isClose: false,
			data:    model,
		}
	}
}

func (bb *BulkBlock) Flush() {
	if bb.isClosed {

	} else {
		bb.chanRequest <- mongoRequest{
			isFlush: true,
			isClose: false,
			data:    nil,
		}
	}
}

// Close - send remain accumulated request.
func (bb *BulkBlock) Close() {
	bb.chanRequest <- mongoRequest{
		isFlush: false,
		isClose: true,
		data:    nil,
	}
}

// String - status message
func (bb *BulkBlock) String() string {
	return fmt.Sprintf("mongo:%v db:%v col:%v", bb.client.mongodbAccess, bb.dbName, bb.collectionName)
}
