package keydb2

import (
	"context"
	"log"
	"net"
	"regexp"
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

var splitAddress = regexp.MustCompile(`^mongodb://(([\w.]+)(:(\d*))?)$`)

// Avail - check mongodb
func Avail(access string) bool {
	tks := splitAddress.FindStringSubmatch(access)
	address := tks[2]
	if len(tks[4]) > 0 {
		address += ":" + tks[4]
	}
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// New - prepare mongodb access
func New(access string) (c *KeyDB) {
	var err error
	iVal, exist := clientMap.Load(access)
	if !exist {
		if Avail(access) {
			var newKeyDB = new(KeyDB)
			newKeyDB.myContext = context.TODO()
			newKeyDB.mongodbAccess = access
			clientOptions := options.Client().ApplyURI(newKeyDB.mongodbAccess)
			if newKeyDB.mongoClient, err = mongo.Connect(newKeyDB.myContext, clientOptions); err != nil {
				log.Fatalf("%v", err)
			}
			if err = newKeyDB.mongoClient.Ping(newKeyDB.myContext, nil); err != nil {
				log.Fatalln(err)
			}
			clientMap.Store(access, newKeyDB)
			c = newKeyDB
		} else {
			log.Fatalf("server '%v' not avail", access)
		}
	} else {
		c = iVal.(*KeyDB)
	}
	return
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
		clientMap.Delete(key)
		return true
	})
}

// Close - remove connection to DB
func (x *KeyDB) Close() {
	if err := x.mongoClient.Disconnect(x.myContext); err != nil {
		log.Printf("close fail: %v", x.mongodbAccess)
	} else {
		log.Printf("close mongo: %v", x.mongodbAccess)
	}
	clientMap.Delete(x.mongodbAccess)
}

// Database - return database with parameter, no admin for this DB
func (x *KeyDB) Database(dbName string) *mongo.Database {
	return x.mongoClient.Database(dbName)
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

func (x *KeyDB) DbNames(requiredCollectionName ...string) []string {
	dbs, err := x.mongoClient.ListDatabases(x.myContext, bson.M{})
	if err != nil {
		log.Fatalf("%v", err)
	}
	var result []string
	for _, db := range dbs.Databases {
		switch db.Name {
		case "admin", "config", "local", "human":
			continue
		default:
			wDB := x.mongoClient.Database(db.Name)
			cols, err := wDB.ListCollectionNames(x.myContext, bson.M{})
			if err != nil {
				log.Fatalf("%v", err)
			}
			if len(cols) < len(requiredCollectionName) {
				continue
			}
			matchCount := 0
			for _, cn := range cols {
				for _, rn := range requiredCollectionName {
					if cn == rn {
						matchCount++
						break
					}
				}
			}
			if matchCount == len(requiredCollectionName) {
				result = append(result, db.Name)
			}
		}
	}
	return result
}
