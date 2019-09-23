package elastic

// There's no good embedded ElasticSearch, so we use a real ElasticSearch instance.
// To enable below tests:
// $ docker pull docker.elastic.co/elasticsearch/elasticsearch:6.4.3
// $ docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.4.3

/*
import (
	"context"
	"fmt"
	"github.com/cortexproject/cortex/pkg/chunk"
	"testing"
)

var config = ElasticConfig{
	Address:   "http://127.0.0.1:9200",
	IndexType: "lokiindex",
}

func TestNewESIndexClient(t *testing.T) {
	NewESIndexClient(config)
}

func TestCreate(t *testing.T) {
	client, _ := NewESIndexClient(config)
	ctx = context.Background()

	writeBatch := client.NewWriteBatch()
	writeBatch.Add("index_2594_test", "fake:d18161:logs:job", nil, nil)
	client.BatchWrite(ctx, writeBatch)
}

func TestQuery(t *testing.T) {
	client, _ := NewESIndexClient(config)
	ctx = context.Background()

	var have int
	queries := []chunk.IndexQuery {chunk.IndexQuery{
		TableName: "index_2594",
		HashValue: "fake:d18162:logs:job",
		RangeValuePrefix: nil,
	},chunk.IndexQuery{
		TableName: "index_2594",
		HashValue: "fake:d18161:logs:job",
		RangeValuePrefix: nil,
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
*/
