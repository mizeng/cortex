package elastic

import (
	"fmt"
	"github.com/cortexproject/cortex/pkg/chunk"
	"testing"
)

//var config = ElasticConfig{
//	Address:   "http://127.0.0.1:9200",
//	IndexName: "loki",
//	IndexType: "lokiindex",
//}
//
//var ctx = context.Background()


func TestCRDTable(t *testing.T) {
	client, _ := NewTableClient(ctx, config)
	result, _  := client.ListTables(ctx)
	fmt.Println(result)

	desc := chunk.TableDesc{
		Name:              "test1",
		UseOnDemandIOMode: false,
		ProvisionedRead:   0,
		ProvisionedWrite:  0,
		Tags:              nil,
		WriteScale:        chunk.AutoScalingConfig{},
		ReadScale:         chunk.AutoScalingConfig{},
	}
	client.CreateTable(ctx, desc)

	result, _  = client.ListTables(ctx)
	fmt.Println(result)

	client.DeleteTable(ctx, "test1")

	result, _  = client.ListTables(ctx)
	fmt.Println(result)
}