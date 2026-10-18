package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/collector/store/dynamostore"
	"github.com/maheshpaliwal/agentic-test-session/collector/store/mongostore"
	"github.com/maheshpaliwal/agentic-test-session/internal/svc"
)

// storeFlags holds every backend's settings; only the selected backend's are used.
// Connection strings may carry credentials, so prefer the environment variables
// over flags (flag values are visible in the process list).
type storeFlags struct {
	kind                                   *string
	pgDSN, pgSchema                        *string
	mongoURI, mongoDB                      *string
	dynamoTable, dynamoRegion, dynamoEndpt *string
	dynamoCreate                           *bool
}

func registerStoreFlags() storeFlags {
	return storeFlags{
		kind:         flag.String("store", svc.Env("TSESSION_COLLECTOR_STORE", "mem"), "store backend: "+strings.Join(storeKinds(), "|")),
		pgDSN:        flag.String("pg-dsn", os.Getenv("TSESSION_COLLECTOR_PG_DSN"), "Postgres DSN (store=pg)"),
		pgSchema:     flag.String("pg-schema", svc.Env("TSESSION_COLLECTOR_PG_SCHEMA", store.DefaultPGSchema), "Postgres schema for trace tables (store=pg)"),
		mongoURI:     flag.String("mongo-uri", os.Getenv("TSESSION_COLLECTOR_MONGO_URI"), "MongoDB connection URI (store=mongo)"),
		mongoDB:      flag.String("mongo-db", svc.Env("TSESSION_COLLECTOR_MONGO_DB", "tsession"), "MongoDB database (store=mongo)"),
		dynamoTable:  flag.String("dynamo-table", svc.Env("TSESSION_COLLECTOR_DYNAMO_TABLE", dynamostore.DefaultTable), "DynamoDB table (store=dynamodb)"),
		dynamoRegion: flag.String("dynamo-region", os.Getenv("TSESSION_COLLECTOR_DYNAMO_REGION"), "AWS region; default from the AWS environment (store=dynamodb)"),
		dynamoEndpt:  flag.String("dynamo-endpoint", os.Getenv("TSESSION_COLLECTOR_DYNAMO_ENDPOINT"), "endpoint override, e.g. a DynamoDB-compatible emulator (store=dynamodb)"),
		dynamoCreate: flag.Bool("dynamo-create-table", os.Getenv("TSESSION_COLLECTOR_DYNAMO_CREATE_TABLE") == "true", "create the table if missing (store=dynamodb)"),
	}
}

// storeOpeners maps a --store value to its constructor.
func (f storeFlags) storeOpeners() map[string]func(context.Context) (store.Store, error) {
	return map[string]func(context.Context) (store.Store, error){
		"mem": func(context.Context) (store.Store, error) { return store.NewMem(), nil },
		"pg": func(ctx context.Context) (store.Store, error) {
			if *f.pgDSN == "" {
				return nil, usageError("store=pg requires --pg-dsn or TSESSION_COLLECTOR_PG_DSN")
			}
			return store.OpenPGConfig(ctx, store.PGConfig{DSN: *f.pgDSN, Schema: *f.pgSchema})
		},
		"mongo": func(ctx context.Context) (store.Store, error) {
			if *f.mongoURI == "" {
				return nil, usageError("store=mongo requires --mongo-uri or TSESSION_COLLECTOR_MONGO_URI")
			}
			return mongostore.Open(ctx, mongostore.Config{URI: *f.mongoURI, Database: *f.mongoDB})
		},
		"dynamodb": func(ctx context.Context) (store.Store, error) {
			return dynamostore.Open(ctx, dynamostore.Config{
				TableName: *f.dynamoTable, Region: *f.dynamoRegion, Endpoint: *f.dynamoEndpt, CreateTable: *f.dynamoCreate,
			})
		},
	}
}

// usageError marks a bad invocation (exit 2) as opposed to a runtime failure (exit 1).
type usageError string

func (e usageError) Error() string { return string(e) }

func storeKinds() []string {
	var ks []string
	for k := range (storeFlags{}).storeOpeners() {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// open builds the selected backend; bad invocations return a usageError.
func (f storeFlags) open(ctx context.Context) (store.Store, error) {
	open, ok := f.storeOpeners()[*f.kind]
	if !ok {
		return nil, usageError(fmt.Sprintf("unknown store %q (want %s)", *f.kind, strings.Join(storeKinds(), "|")))
	}
	return open(ctx)
}
