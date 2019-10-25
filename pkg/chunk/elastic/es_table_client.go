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
	// Here we use ElasticSearch auto index management with templates, no need to create table manually.
	// Instead, we create Template.
	templateName := "loki-index"
	// check if template exists, and create one if not exist
	exists, err := c.client.IndexTemplateExists(templateName).Do(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	if !exists {
		tmpl := `{
		"index_patterns":["lokiindex_*"],
		"settings":{
			"number_of_shards":3,
			"number_of_replicas":1
		},
		"mappings":{
			"lokiindex": {
				"properties": {
					"hash": {
						"type": "keyword"
					},
					"range": {
						"type": "keyword"
					},
					"value": {
						"type": "keyword"
					}
				}
			}
		}
	}`
		_, err := c.client.IndexPutTemplate(templateName).BodyString(tmpl).Do(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
	}

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
