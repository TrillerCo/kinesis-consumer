package connector

import (
	"fmt"
	"time"

	"github.com/sendgridlabs/go-kinesis"
)

// Pipeline is used as a record processor to configure a pipline.
//
// The user should implement this such that each method returns a configured implementation of each
// interface. It has a data type (Model) as Records come in as a byte[] and are transformed to a Model.
// Then they are buffered in Model form and when the buffer is full, Models's are passed to the emitter.
type Pipeline struct {
	Buffer      Buffer
	Checkpoint  Checkpoint
	Emitter     Emitter
	Filter      Filter
	StreamName  string
	Transformer Transformer
}

// ProcessShard kicks off the process of a Kinesis Shard.
// It is a long running process that will continue to read from the shard.
func (p Pipeline) ProcessShard(ksis *kinesis.Kinesis, shardID string) {
	args := kinesis.NewArgs()
	args.Add("ShardId", shardID)
	args.Add("StreamName", p.StreamName)

	if p.Checkpoint.CheckpointExists(shardID) {
		args.Add("ShardIteratorType", "AFTER_SEQUENCE_NUMBER")
		args.Add("StartingSequenceNumber", p.Checkpoint.SequenceNumber())
	} else {
		args.Add("ShardIteratorType", "TRIM_HORIZON")
	}

	shardInfo, err := ksis.GetShardIterator(args)

	if err != nil {
		fmt.Printf("Error fetching shard itterator: %v", err)
		return
	}

	shardIterator := shardInfo.ShardIterator

	for {
		args = kinesis.NewArgs()
		args.Add("ShardIterator", shardIterator)
		recordSet, err := ksis.GetRecords(args)

		if err != nil {
			fmt.Printf("GetRecords ERROR: %v\n", err)
			time.Sleep(10 * time.Second)
			continue
		}

		if len(recordSet.Records) > 0 {
			for _, v := range recordSet.Records {
				data := v.GetData()

				if err != nil {
					fmt.Printf("GetData ERROR: %v\n", err)
					continue
				}

				r := p.Transformer.ToRecord(data)

				if p.Filter.KeepRecord(r) {
					p.Buffer.ProcessRecord(r, v.SequenceNumber)
				}
			}
		} else if recordSet.NextShardIterator == "" || shardIterator == recordSet.NextShardIterator || err != nil {
			fmt.Printf("NextShardIterator ERROR: %v\n", err)
			break
		} else {
			fmt.Printf("Sleeping: %v\n", shardID)
			time.Sleep(10 * time.Second)
		}

		if p.Buffer.ShouldFlush() {
			fmt.Printf("Emitting to Shard: %v\n", shardID)
			p.Emitter.Emit(p.Buffer, p.Transformer)
			p.Checkpoint.SetCheckpoint(shardID, p.Buffer.LastSequenceNumber())
			p.Buffer.Flush()
		}

		shardIterator = recordSet.NextShardIterator
	}
}
