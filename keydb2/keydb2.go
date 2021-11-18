package keydb2

import (
	"context"
	"log"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var clientMap sync.Map // make(map[string]*KeyDB)

type KeyDB struct {
	myContext     context.Context //= context.Background()
	mongodbAccess string
	mongoClient   *mongo.Client //= nil
	mapCollection sync.Map      // map[string]*mongo.Collection
	mapBulk       sync.Map      // map[string]*BulkBlock
}

// New - prepare mongodb access
func New(access string) *KeyDB {
	var err error
	iVal, exist := clientMap.Load(access)
	if !exist {
		var newKeyDB KeyDB
		newKeyDB.myContext = context.Background()
		newKeyDB.mongodbAccess = access
		clientOptions := options.Client().ApplyURI(newKeyDB.mongodbAccess)
		if newKeyDB.mongoClient, err = mongo.Connect(newKeyDB.myContext, clientOptions); err != nil {
			log.Fatalln(err)
		}
		if err = newKeyDB.mongoClient.Ping(newKeyDB.myContext, nil); err != nil {
			log.Fatalln(err)
		}
		clientMap.Store(access, &newKeyDB)
		return &newKeyDB
	} else {
		return iVal.(*KeyDB)
	}
}

// GoodBye - disconnect all connection
func GoodBye() {
	clientMap.Range(func(key, value interface{}) bool {
		kdb := value.(*KeyDB)
		if err := kdb.mongoClient.Disconnect(kdb.myContext); err != nil {
			log.Printf("%v %v", kdb.mongodbAccess, err)
		} else {
			log.Printf("disconnect %v", kdb.mongodbAccess)
		}
		return true
	})
}

// Col - return collection, if not exist make collection and return it.
func (x *KeyDB) Col(dbName, collectionName string) *mongo.Collection {
	dbCol := dbName + "::" + collectionName
	iVal, exist := x.mapCollection.Load(dbCol)
	if exist {
		return iVal.(*mongo.Collection)
	} else {
		collection := x.mongoClient.Database(dbName).Collection(collectionName)
		x.mapCollection.Store(dbCol, collection)
		return collection
	}
}

// Drop - delete collection
func (x *KeyDB) Drop(dbName string, collectionNames ...string) {
	for _, colName := range collectionNames {
		dbCol := dbName + "::" + colName
		iVal, exist := x.mapCollection.Load(dbCol)
		if exist {
			if err := iVal.(*mongo.Collection).Drop(x.myContext); err != nil {
				log.Fatalf("%v drop error. %v", dbCol, err)
			}
		} else {
			if err := x.mongoClient.Database(dbName).Collection(colName).Drop(x.myContext); err != nil {
				log.Fatalf("%v drop error. %v", dbCol, err)
			}
		}
		x.mapCollection.Delete(dbCol)
		x.mapBulk.Delete(dbCol)
	}
}

// DropDb - Delete DB and associated collection.
func (x *KeyDB) DropDb(dbName string) {
	if err := x.mongoClient.Database(dbName).Drop(x.myContext); err != nil {
		log.Fatalln(err)
	}
	x.mapCollection.Range(func(key, value interface{}) bool {
		if strings.HasPrefix(key.(string), dbName) {
			x.mapCollection.Delete(key.(string))
		}
		return true
	})
	x.mapBulk.Range(func(key, value interface{}) bool {
		if strings.HasPrefix(key.(string), dbName) {
			x.mapBulk.Delete(key.(string))
		}
		return true
	})
}

// Index - add index definition. Specify key elements as repeated string.
func (x *KeyDB) Index(dbName, collectionName string, fieldName ...string) {
	collection := x.Col(dbName, collectionName)
	var vFalse = false
	var vTrue = true
	var keyDef bson.D
	indexName := dbName + "_" + collectionName
	for _, kf := range fieldName {
		indexName += "_" + kf
		keyDef = append(keyDef, bson.E{Key: kf, Value: 1})
	}
	model := mongo.IndexModel{
		Keys: keyDef,
		Options: &options.IndexOptions{
			Name:   &indexName,
			Unique: &vFalse,
			Sparse: &vTrue,
		},
	}
	if _, err := collection.Indexes().CreateOne(x.myContext, model); err != nil {
		log.Printf("%v", err)
	}
}

// IndexUnique - add index definition. Specify key elements as repeated string.
func (x *KeyDB) IndexUnique(dbName, collectionName string, fieldName ...string) {
	collection := x.Col(dbName, collectionName)
	var vTrue = true
	var keyDef bson.D
	indexName := dbName + "_" + collectionName
	for _, kf := range fieldName {
		indexName += "_" + kf
		keyDef = append(keyDef, bson.E{Key: kf, Value: 1})
	}
	model := mongo.IndexModel{
		Keys: keyDef,
		Options: &options.IndexOptions{
			Name:   &indexName,
			Unique: &vTrue,
			Sparse: &vTrue,
		},
	}
	if _, err := collection.Indexes().CreateOne(x.myContext, model); err != nil {
		log.Printf("%v", err)
	}
}
