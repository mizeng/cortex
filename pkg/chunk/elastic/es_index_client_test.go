package elastic

import (
	"context"
	"fmt"
	"github.com/cortexproject/cortex/pkg/chunk"
	"testing"
)

var config = ElasticConfig{
	Address:   "http://127.0.0.1:9200",
	IndexName: "loki",
	IndexType: "lokiindex",
}

func TestNewESIndexClient(t *testing.T) {
	NewESIndexClient(config)
}

func TestCreate(t *testing.T) {
	client, _ := NewESIndexClient(config)
	client.BatchWrite(nil, nil)
}

func TestQuery(t *testing.T) {
	client, _ := NewESIndexClient(config)
	ctx = context.Background()
	queries:= []chunk.IndexQuery {chunk.IndexQuery{
		TableName: "tableName",
		HashValue: "olivere",
		RangeValueStart: []byte("Take Five1"),
	}}

	var have int
	client.QueryPages(ctx, queries, func(_ chunk.IndexQuery, read chunk.ReadBatch) bool {
		iter := read.Iterator()
		for iter.Next() {
			have++
		}
		return true
	})
	fmt.Printf("have %d\n", have)

	have = 0
	queries= []chunk.IndexQuery {chunk.IndexQuery{
		TableName: "tableName",
		HashValue: "olivere",
		RangeValuePrefix: []byte("Take Five2"),
	}}
	client.QueryPages(ctx, queries, func(_ chunk.IndexQuery, read chunk.ReadBatch) bool {
		iter := read.Iterator()
		for iter.Next() {
			have++
		}
		return true
	})
	fmt.Printf("have %d\n", have)
}