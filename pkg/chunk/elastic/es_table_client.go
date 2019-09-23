package elastic

import (
	"context"
	"github.com/olivere/elastic"

	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/chunk"
)

//var ctx = context.Background()

type tableClient struct {
	cfg    Config
	client *elastic.Client
}

// NewTableClient returns a new TableClient.
func NewTableClient(ctx context.Context, cfg Config) (chunk.TableClient, error) {
	client, err := newES(cfg)
	if err != nil {
		return nil, err
	}
	return &tableClient{
		cfg:    cfg,
		client: client,
	}, nil
}

// ListTables means list index in ElasticSearch
func (c *tableClient) ListTables(ctx context.Context) ([]string, error) {
	response, err := c.client.IndexNames()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return response, nil
}

func (c *tableClient) CreateTable(ctx context.Context, desc chunk.TableDesc) error {
	//// Use the IndexExists service to check if a specified index exists.
	//exists, err := c.client.IndexExists(desc.Name).Do(ctx)
	//if err != nil {
	//	return errors.WithStack(err)
	//}
	//if !exists {
	//	// Create a new index.
	//	createIndex, err := c.client.CreateIndex(desc.Name).BodyString(mapping).Do(ctx)
	//	if err != nil {
	//		return errors.WithStack(err)
	//	}
	//	if !createIndex.Acknowledged {
	//		// Not acknowledged
	//	}
	//}
	return nil
}

func (c *tableClient) DeleteTable(ctx context.Context, name string) error {
	// Delete an index.
	_, err := client.DeleteIndex(name).Do(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *tableClient) DescribeTable(ctx context.Context, name string) (desc chunk.TableDesc, isActive bool, err error) {
	return chunk.TableDesc{
		Name: name,
	}, true, nil
}

func (c *tableClient) UpdateTable(ctx context.Context, current, expected chunk.TableDesc) error {
	return nil
}
