package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	sourceURI      = ""
	destinationURI = ""
)

// List of default databases
var excludeDatabases = map[string]bool{
	"admin":  true,
	"config": true,
	"local":  true,
	"test":   true, // Remove this line if you want to copy the "test" database
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sourceClient, err := mongo.Connect(ctx, options.Client().ApplyURI(sourceURI))
	if err != nil {
		fmt.Printf("Error connecting to source DB: %s\n", err)
		return
	}
	defer sourceClient.Disconnect(ctx)

	destinationClient, err := mongo.Connect(ctx, options.Client().ApplyURI(destinationURI))
	if err != nil {
		fmt.Printf("Error connecting to destination DB: %s\n", err)
		return
	}
	defer destinationClient.Disconnect(ctx)

	if err := sourceClient.Ping(ctx, readpref.Primary()); err != nil {
		fmt.Printf("Ping to source DB failed: %s\n", err)
	}
	if err := destinationClient.Ping(ctx, readpref.Primary()); err != nil {
		fmt.Printf("Ping to destination DB failed: %s\n", err)
	}

	databases, err := sourceClient.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		fmt.Printf("Failed to list databases: %s\n", err.Error())
		return
	}

	for _, dbName := range databases {
		if excludeDatabases[dbName] {
			continue
		}

		collections, err := sourceClient.Database(dbName).ListCollectionNames(ctx, bson.D{})
		if err != nil {
			fmt.Printf("Failed to list collections of DB '%s': %s\n", dbName, err.Error())
			return
		}

		for _, colName := range collections {
			sourceCollection := sourceClient.Database(dbName).Collection(colName)
			destCollection := destinationClient.Database(dbName).Collection(colName)

			cursor, err := sourceCollection.Find(ctx, bson.D{})
			if err != nil {
				fmt.Printf("Failed to find documents in collection '%s' of DB '%s': Error: %s\n", colName, dbName, err.Error())
				return
			}
			defer cursor.Close(ctx)

			var documents []interface{}

			if err = cursor.All(ctx, &documents); err != nil {
				fmt.Printf("Failed to retrieve documents from collection '%s' of DB '%s': Error: %s\n", colName, dbName, err.Error())
				return
			}

			if _, err = destCollection.DeleteMany(ctx, bson.D{}); err != nil {
				fmt.Printf("Failed to delete existing documents in destination collection '%s' of DB '%s': Error: %s\n", colName, dbName, err.Error())
				return
			}

			if len(documents) > 0 {
				_, err = destCollection.InsertMany(ctx, documents)
				if err != nil {
					fmt.Printf("Failed to insert documents in destination collection '%s' of DB '%s': Error: %s\n", colName, dbName, err.Error())
					return
				}
			}

			fmt.Printf("Collection '%s' of DB '%s' transferred.\n", colName, dbName)
		}
	}
}
