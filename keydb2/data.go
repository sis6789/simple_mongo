package keydb2

import (
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
)

var insertIfNoRec = options.Update().SetUpsert(true)

// Insert - add new document func InsertMongo[TMongo any](db, col string, pMongo *TMongo) (err error) {
func Insert[TData any](x *KeyDB, db, col string, pData *TData) (err error) {
	wCollection := x.Col(db, col)
	_, err = wCollection.InsertOne(x.myContext, pData)
	if err != nil {
		log.Printf("%v", err)
	}
	return
}

// InsertMany - add new document func InsertMongo[TMongo any](db, col string, pMongo *TMongo) (err error) {
func InsertMany[TData any](x *KeyDB, db, col string, pData []TData) (err error) {
	wCollection := x.Col(db, col)
	var dataSet []interface{}
	for ix := 0; ix < len(pData); ix++ {
		dataSet = append(dataSet, interface{}(pData[ix]))
	}
	opts := options.InsertMany().SetOrdered(false)
	opExit, err := wCollection.InsertMany(x.myContext, dataSet, opts)
	if err != nil {
		switch err.(type) {
		case mongo.BulkWriteException:
			e1 := err.(mongo.BulkWriteException)
			failed := len(e1.WriteErrors)
			err = errors.New(fmt.Sprintf("insert failed:%d, success:%d", failed, len(pData)-failed))
		}
	} else if len(opExit.InsertedIDs) != len(pData) {
		success := len(opExit.InsertedIDs)
		failed := len(pData) - success
		err = errors.New(fmt.Sprintf("insert failed:%d, success:%d", failed, success))
	}
	return
}

// Update - add new field or update
func Update[TKey, TData any](x *KeyDB, db, col string, pKey TKey, pData *TData) (err error) {
	wCollection := x.Col(db, col)
	_, err = wCollection.UpdateOne(x.myContext, bson.M{"_id": pKey}, bson.M{"$set": *pData}, insertIfNoRec)
	if err != nil {
		log.Fatalf("%v", err)
	}
	return
}

// Add - add new field or update
func Add[TKey, TData any](x *KeyDB, db, col string, pKey TKey, pData *TData) (err error) {
	wCollection := x.Col(db, col)
	_, err = wCollection.UpdateOne(x.myContext, bson.M{"_id": pKey}, bson.M{"$set": *pData}, insertIfNoRec)
	if err != nil {
		log.Fatalf("%v", err)
	}
	return
}

// Delete - delete document
func Delete[TKey any](x *KeyDB, db, col string, tKey TKey) (err error) {
	wCollection := x.Col(db, col)
	_, err = wCollection.DeleteOne(x.myContext, bson.M{"_id": tKey})
	return
}

// Bulk - request mixed operation
func Bulk(x *KeyDB, db, col string, models []mongo.WriteModel) (err error) {
	wCollection := x.Col(db, col)
	_, err = wCollection.BulkWrite(x.myContext, models)
	return
}
