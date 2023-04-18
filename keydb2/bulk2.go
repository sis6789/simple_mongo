package keydb2

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/sis6789/nucs/limitGoSub"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BulkBlock struct {
	limit               int
	dbName              string
	collectionName      string
	collection          *mongo.Collection
	chanRequest         chan mongoRequest
	requestReceiverSync sync.WaitGroup
	isClosed            bool
	client              *KeyDB
	onceClose           sync.Once
	limitMaxIssue       *limitGoSub.LimitGoSub
	bulkWriteOption     *options.BulkWriteOptions
}

type mongoRequest struct {
	isFlush bool
	isClose bool
	data    mongo.WriteModel
}

// merger - 야러 고루틴에서 보내지는 요구를 모아서 DB에 적용한다.
func requestReceiver(bb *BulkBlock) {
	defer bb.requestReceiverSync.Done()
	var requestStorage []mongo.WriteModel
	var mongoCallSync sync.WaitGroup
	// define mongo db request
	issueMongoCommand := func(models []mongo.WriteModel) {
		defer func() {
			bb.limitMaxIssue.Done()
			mongoCallSync.Done()
		}()
		bb.limitMaxIssue.Wait()
		if _, err := bb.collection.BulkWrite(context.Background(), models, bb.bulkWriteOption); err != nil {
			log.Printf("%v", err)
		}
	}
	// loop for request
	for request := range bb.chanRequest {
		if request.isFlush {
			if len(requestStorage) > 0 {
				mongoCallSync.Add(1)
				go issueMongoCommand(requestStorage)
				requestStorage = []mongo.WriteModel{}
			}
		} else if request.isClose {
			break
		} else {
			requestStorage = append(requestStorage, request.data)
			if len(requestStorage) >= bb.limit {
				mongoCallSync.Add(1)
				go issueMongoCommand(requestStorage)
				requestStorage = []mongo.WriteModel{}
			}
		}
	}
	// process remain request
	if len(requestStorage) > 0 {
		mongoCallSync.Add(1)
		go issueMongoCommand(requestStorage)
		requestStorage = []mongo.WriteModel{}
	}
	// 요구한 몽고 DB 처리 완료를 기다린다.
	mongoCallSync.Wait()
	bb.isClosed = true
	log.Printf("bulk close: %v", bb)
}

// NewBulk - prepare bulk operation
func (x *KeyDB) NewBulk(dbName, collectionName string, interval int) *BulkBlock {
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
		pB.requestReceiverSync.Add(1)
		pB.limitMaxIssue = limitGoSub.New(3)
		pB.bulkWriteOption = options.BulkWrite().SetOrdered(false)
		go requestReceiver(pB)
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
		// create new channel
		var b BulkBlock
		pB := &b
		initializeBlock(pB)
		log.Printf("bulk start: %v", pB)
		x.mapBulk.Swap(dbCol, pB)
		return pB
	}
}

func (bb *BulkBlock) SetOrderedWrite(order bool) {
	bb.bulkWriteOption = options.BulkWrite().SetOrdered(order)
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
	bb.requestReceiverSync.Wait()
}

// String - status message
func (bb *BulkBlock) String() string {
	return fmt.Sprintf("mongo:%v db:%v col:%v", bb.client.mongodbAccess, bb.dbName, bb.collectionName)
}
