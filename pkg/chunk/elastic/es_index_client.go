package elastic

import (
	"context"
	"flag"
	"github.com/cortexproject/cortex/pkg/chunk"
	chunk_util "github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"

	"reflect"
	"strconv"

	"fmt"
	"github.com/olivere/elastic"
)

const (
	null           = string('\xff')
	max_fetch_docs = 1000
)

// BoltDBConfig for a BoltDB index client.
type ElasticConfig struct {
	Address 	string 	`yaml:"address"`
	IndexName 	string 	`yaml:"index_name"`
	IndexType 	string 	`yaml:"index_type"`
}

// RegisterFlags registers flags.
func (cfg *ElasticConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Address, "elastic.address", "http://127.0.0.1:9200", "Address of ElasticSearch.")
	f.StringVar(&cfg.IndexName, "elastic.index_name", "loki", "Index Name used in ElasticSearch.")
	f.StringVar(&cfg.IndexType, "elastic.index_type", "lokiindex", "Index Type used in ElasticSearch.")
}

// Tweet is a structure used for serializing/deserializing data in Elasticsearch.
type LokiIndex struct {
	Hash    string		`json:"hash"`
	Range  	string		`json:"range"`
	Value   string		`json:"value,omitempty"`
}

const mapping = `
{
	"settings":{
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings":{
		"lokiindex":{
			"properties":{
				"hash":{
					"type":"keyword"
				},
				"range":{
					"type":"keyword"
				},
				"value":{
					"type":"keyword"
				}
			}
		}
	}
}`

var client *elastic.Client
// Starting with elastic.v5, you must pass a context to execute each service
var ctx = context.Background()

// StorageClient implements chunk.IndexClient and chunk.ObjectClient for Cassandra.
type esClient struct {
	cfg 		ElasticConfig
	client   	*elastic.Client
}

func (e *esClient) Stop() {
	e.client.Stop()
}

// ES batching isn't really useful in this case, its more to do multiple
// atomic writes.  Therefore we just do a bunch of writes in parallel.
type writeBatch struct {
	entries []chunk.IndexEntry
}

func (e *esClient) NewWriteBatch() chunk.WriteBatch {
	return &writeBatch{}
}

func (b *writeBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	b.entries = append(b.entries, chunk.IndexEntry{
		TableName:  tableName,
		HashValue:  hashValue,
		RangeValue: rangeValue,
		Value:      value,
	})
}

func (e *esClient) BatchWrite(ctx context.Context, batch chunk.WriteBatch) error {
	b := batch.(*writeBatch)

	exists, err := e.client.IndexExists(e.cfg.IndexName).Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if !exists {
		// Create a new index.
		createIndex, err := e.client.CreateIndex(e.cfg.IndexName).BodyString(mapping).Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	bulkRequest := e.client.Bulk()
	for _, entry := range b.entries {
		loki := LokiIndex{Hash: entry.HashValue, Range: string(entry.RangeValue), Value: string(entry.Value)}
		req := elastic.NewBulkIndexRequest().Index(e.cfg.IndexName).Type(e.cfg.IndexType).Doc(loki)
		bulkRequest = bulkRequest.Add(req)
	}

	bulkResponse, err := bulkRequest.Do(ctx)
	if err != nil {
		return err
	}

	if bulkResponse != nil {

	}
	return nil
}

// readBatch represents a batch of rows read from ElasticSearch.
type readBatch struct {
	rangeValue []byte
	value      []byte
}

func (r readBatch) Iterator() chunk.ReadBatchIterator {
	return &elasticReadBatchIterator{
		readBatch: r,
	}
}

type elasticReadBatchIterator struct {
	consumed bool
	readBatch
}

func (r *elasticReadBatchIterator) Next() bool {
	if r.consumed {
		return false
	}
	r.consumed = true
	return true
}

func (r *elasticReadBatchIterator) RangeValue() []byte {
	return r.rangeValue
}

func (r *elasticReadBatchIterator) Value() []byte {
	return r.value
}

func (e *esClient) QueryPages(ctx context.Context, queries []chunk.IndexQuery, callback func(chunk.IndexQuery, chunk.ReadBatch) (shouldContinue bool)) error {
	return chunk_util.DoParallelQueries(ctx, e.query, queries, callback)
}

func (e *esClient) query(ctx context.Context, query chunk.IndexQuery, callback func(chunk.ReadBatch) (shouldContinue bool)) error {
	var rangeQuery *elastic.RangeQuery
	var valueTermQuery *elastic.TermQuery

	hashTermQuery := elastic.NewTermQuery("hash", query.HashValue)
	switch {
	case len(query.RangeValuePrefix) > 0 && query.ValueEqual == nil:
		rangeQuery = elastic.NewRangeQuery("range").Gte(string(query.RangeValuePrefix)).
			Lt(string(query.RangeValuePrefix) + null)

	case len(query.RangeValuePrefix) > 0 && query.ValueEqual != nil:
		rangeQuery = elastic.NewRangeQuery("range").Gte(string(query.RangeValuePrefix)).
			Lt(string(query.RangeValuePrefix) + null)
		valueTermQuery= elastic.NewTermQuery("value", query.ValueEqual)

	case len(query.RangeValueStart) > 0 && query.ValueEqual == nil:
		rangeQuery = elastic.NewRangeQuery("range").Gte(string(query.RangeValueStart))

	case len(query.RangeValueStart) > 0 && query.ValueEqual != nil:
		rangeQuery = elastic.NewRangeQuery("range").Gte(string(query.RangeValueStart))
		valueTermQuery= elastic.NewTermQuery("value", query.ValueEqual)

	case query.ValueEqual != nil:
		valueTermQuery= elastic.NewTermQuery("value", query.ValueEqual)

	case query.ValueEqual == nil:
		break
	}
	// Search with a term query
	searchResult, err := e.client.Search().
		Index(e.cfg.IndexName).   // search in index"
		Query(hashTermQuery). // specify the query
		Query(valueTermQuery).
		Query(rangeQuery).
		Sort("range", true). // sort by "range" field, ascending
		From(0).Size(max_fetch_docs).   // take documents 0-9
		Pretty(true).       // pretty print request and response JSON
		Do(ctx)             // execute
	if err != nil {
		// Handle error
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	level.Debug(util.Logger).Log("msg", fmt.Sprintf("Query took %d milliseconds\n", searchResult.TookInMillis))

	var batch readBatch
	var ttyp LokiIndex
	for _, item := range searchResult.Each(reflect.TypeOf(ttyp)) {
		if t, ok := item.(LokiIndex); ok {
			level.Debug(util.Logger).Log("msg", fmt.Sprintf("LokiIndex by hash %s: range %s, value %s\n", t.Hash, t.Range, t.Value))
			batch.rangeValue = []byte(t.Range)
			batch.value = []byte(t.Value)

			if !callback(&batch) {
				return nil
			}
		}
	}
	// TotalHits is another convenience function that works even when something goes wrong.
	//fmt.Printf("Found a total of %d tweets\n", searchResult.TotalHits())

	return nil
}

func NewESIndexClient(cfg ElasticConfig) (chunk.IndexClient, error){
	client, err := newES(cfg)
	if err != nil {
		return nil, err
	}
	indexClient := &esClient{
		cfg,
		client,
	}
	return indexClient, nil
}

func newES(cfg ElasticConfig) (*elastic.Client, error) {
	// Obtain a client and connect to the default Elasticsearch installation
	// on 127.0.0.1:9200. Of course you can configure your client to connect
	// to other hosts and configure it in various other ways.
	var err error
	client, err = elastic.NewClient(elastic.SetURL(cfg.Address), elastic.SetSniff(false))
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (e *esClient) CreateIndex() {
	// Use the IndexExists service to check if a specified index exists.
	exists, err := e.client.IndexExists(e.cfg.IndexName).Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if !exists {
		// Create a new index.
		createIndex, err := e.client.CreateIndex(e.cfg.IndexName).BodyString(mapping).Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	// Index a tweet (using JSON serialization)
	loki1 := LokiIndex{Hash: "olivere", Range: "Take Five", Value: "0"}
	put1, err := e.client.Index().
		Index(e.cfg.IndexName).
		Type(e.cfg.IndexType).
		Id("1").
		BodyJson(loki1).
		Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Indexed tweet %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
}

func (e *esClient) CreateBatchIndex() {
	// Use the IndexExists service to check if a specified index exists.
	exists, err := e.client.IndexExists(e.cfg.IndexName).Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if !exists {
		// Create a new index.
		createIndex, err := e.client.CreateIndex(e.cfg.IndexName).BodyString(mapping).Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	bulkRequest := e.client.Bulk()
	for i := 0; i < 10; i++ {
		loki := LokiIndex{Hash: "olivere", Range: "Take Five" + strconv.Itoa(i), Value: "0"}
		req := elastic.NewBulkIndexRequest().Index(e.cfg.IndexName).Type(e.cfg.IndexType).Doc(loki)
		bulkRequest = bulkRequest.Add(req)
	}

	bulkResponse, err := bulkRequest.Do(ctx)
	if err != nil {
		fmt.Println(err)
	}
	if bulkResponse != nil {

	}
}