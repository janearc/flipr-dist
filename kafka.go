package main

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/janearc/big-little-mesh/consume"
	"github.com/janearc/big-little-mesh/emit"
	"github.com/janearc/big-little-mesh/frood"
	observabilityproto "github.com/janearc/big-little-mesh/proto/observability/v1"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	oplogpb "github.com/janearc/flipr-dist/gen/oplogpb"
)

// The kafka sink: the oplog's immutable home, in the mesh's ONE wire format.
//
// The oplog goes to kafka so it can be considered immutable. Kafka is
// append-only; the record there is the
// tamper-evident audit and the replay source that makes a store wipe cost
// nothing.
//
// Records are Confluent-framed schema-registry protobuf, the same path the
// bus's own heartbeats ride.
//
// An earlier revision argued itself into plain protojson to avoid the
// registry at produce time, and every record was then counted
// off-contract, which is the contract working.
//
// The argument dissolved on inspection: the registry is furnished with
// kafka, which already sits beneath flipr in the boot order, and the schema
// id is cached after first use.
//
// The wire is the boundary-enforcer. Flipr conforms like everybody else.
//
// Sync on the write path: a flip is durable and acked by the broker before
// its caller is answered, or the caller gets an error. That is what 100%
// means.

//go:embed proto/flipr/oplog/v1/oplog.proto
var oplogSchema string

// oplogSubject is the registry subject for the topic's value schema.
const oplogSubject = "flipr.oplog-value"

// KafkaSink implements Sink by publishing framed Operation records through
// blm/emit. Single partition by furnishing; replay depends on total order.
type KafkaSink struct {
	pub        *emit.Publisher
	topic      string
	generation string
	log        *Logger
}

// NewKafkaSink connects the publisher and starts the frood heartbeat on the
// same session -- presence is authorization (hm's lease model), so the
// heartbeat is flipr's citizenship and it costs one goroutine.
func NewKafkaSink(
	ctx context.Context,
	brokers []string,
	registryURL, topic, generation string,
	lg *Logger,
) (*KafkaSink, error) {
	pub, err := emit.New(ctx, brokers, registryURL)
	if err != nil {
		return nil, fmt.Errorf("kafka sink: %w", err)
	}
	// The heartbeat's schemaText is the observability contract, not ours:
	//
	// the heartbeat subject already holds that schema under FULL_TRANSITIVE
	// compatibility, so posting any other text there is refused -- which is
	// exactly what happened to the first revision of this line, and why
	// flipr was absent from hm.test/truth while everything else worked.
	go frood.Heartbeat(
		ctx,
		pub,
		"flipr",
		observabilityproto.Schema,
		30*time.Second,
		slog.Default(),
	)
	return &KafkaSink{
		pub:        pub,
		topic:      topic,
		generation: generation,
		log:        lg,
	}, nil
}

// record converts an oplog entry map into the contract message; the file
// sink uses the same conversion, so both carry one record.
func (k *KafkaSink) record(e map[string]any) *oplogpb.Operation {
	return recordFromEntry(e, k.generation)
}

// Write publishes one framed record synchronously: schema id (cached after
// first use), Confluent frame, produce, broker ack, then return.
func (k *KafkaSink) Write(e map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	err := k.pub.Publish(
		ctx, k.topic, oplogSubject, oplogSchema, "", k.record(e),
	)
	if err != nil {
		return fmt.Errorf("oplog publish: %w", err)
	}
	return nil
}

// Close releases the publisher.
func (k *KafkaSink) Close() { k.pub.Close() }

// Replay consumes the topic from the beginning and hands every SetFlag to
// apply, oldest first. Called ONLY on a fresh store generation, before the
// listener opens.
//
// The end offset is read once, at start, and replay is done when it is Reached.
// It used to be done when a fetch came back empty for one second -- which is
// also exactly what a broker that is slow to answer its first fetch looks like.
//
// A cold kafka therefore made replay report success with nothing restored, and
// flipr opened its listener on an empty store, healthy, serving 404 to every
// flag read in the mesh. The end offset is a fact the broker states; "quiet for
// a second" was a guess.
//
// A record that cannot be applied is a refusal, NOT a warning. Replay used to
// log a record it could not parse or apply and carry on, returning success with
// fewer flips than the log holds.
//
// A store that is missing an operator's flip and reports itself restored is the
// failure this service exists to prevent, so a skipped record makes replay fail
// and flipr refuses to start. The operator then knows exactly which offset to
// look at.
//
// The topic holds two generations of record and replay tolerates both: the
// current Confluent-framed protobuf, and the early unframed protojson
// records from before the sink conformed (they are immutable; pretending
// they are not there would un-restore any flip they carry).
func Replay(ctx context.Context, brokers []string, topic string, lg *Logger,
	apply func(service, version, key, value string, typed bool) error,
	remove func(service, version string) error) (int, error) {

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return 0, fmt.Errorf("replay client: %w", err)
	}
	defer cl.Close()

	targets, next, total, err := replayBounds(ctx, cl, topic)
	if err != nil {
		return 0, err
	}
	if total == 0 {
		lg.Info(
			"op-log replay: the topic holds no records; nothing "+
				"to restore",
			map[string]any{"topic": topic},
		)
		return 0, nil
	}
	lg.Info(
		"op-log replay: reading to the end offset taken at start",
		map[string]any{
			"topic":      topic,
			"records":    total,
			"partitions": len(targets)},
	)

	applied, skipped := 0, 0
	var seen int64
	budget := replayDeadline(total)
	deadline := time.Now().Add(budget)
	lastProgress := time.Now()
	lastLogged := int64(0)
	for !replayCaughtUp(next, targets) {
		if time.Now().After(deadline) {
			return applied, fmt.Errorf(
				"replay did not reach the end offset within "+
					"%s (%d of %d records seen); "+
					"refusing to serve a half-restored "+
					"store",
				budget,
				seen,
				total,
			)
		}
		if time.Since(lastProgress) > replayStall {
			return applied, fmt.Errorf(
				"replay stalled: no record for %s with %d of "+
					"%d still to read; refusing to serve "+
					"a half-restored store",
				replayStall,
				total-seen,
				total,
			)
		}
		pollCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		for _, e := range fetches.Errors() {
			if errors.Is(e.Err, context.DeadlineExceeded) {
				continue
			}
			return applied, fmt.Errorf("replay fetch: %w", e.Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			next[r.Partition] = r.Offset + 1
			seen++
			lastProgress = time.Now()
			if seen-lastLogged >= replayLogEvery {
				lastLogged = seen
				lg.Info(
					"op-log replay: progress",
					map[string]any{
						"seen":    seen,
						"of":      total,
						"applied": applied,
					},
				)
			}
			op, err := decodeOperation(r.Value)
			if err != nil {
				skipped++
				lg.Error(
					"replay: unparseable record; the "+
						"store will be refused",
					map[string]any{
						"offset":    r.Offset,
						"partition": r.Partition,
						"err":       err.Error()},
				)
				return
			}
			switch op.Op {
			case "DeleteNamespace":
				// deletes replay too, in order -- without this,
				// every retired namespace would resurrect from
				// the immutable log at the next wipe-recovery
				err := remove(op.Service, op.Version)
				if err != nil {
					skipped++
					lg.Error(
						"replay: delete failed; the "+
							"store will be "+
							"refused",
						map[string]any{
							"offset":  r.Offset,
							"service": op.Service,
							"version": op.Version,
							"err":     err.Error()},
					)
					return
				}
				applied++
			case "SetFlag":
				payload := op.ValueJson
				if payload == "" {
					payload = op.Value
				}
				// game: allow width
				if err := apply(op.Service, op.Version, op.Key, payload, op.ValueJson != ""); err != nil {
					skipped++
					lg.Error(
						"replay: apply failed; the "+
							"store will be "+
							"refused",
						map[string]any{
							"offset":  r.Offset,
							"service": op.Service,
							"key":     op.Key,
							"err":     err.Error()},
					)
					return
				}
				applied++
			default:
				// reads, publishes and pings carry nothing to
				// restore
			}
		})
	}
	if skipped > 0 {
		return applied, fmt.Errorf(
			"replay skipped %d of %d records it could not parse "+
				"or apply; refusing to serve a store that is "+
				"missing them",
			skipped,
			total,
		)
	}
	return applied, nil
}

// replayBase is the time replay is allowed before the first record counts,
// covering a broker that is slow to answer at all; replayPerRecord is the
// allowance per record between the start and end offsets.
//
// Measured on 2026-09-03: a replayed flip costs one bbolt commit and one
// snapshot rebuild, 1.2ms in the live pod and 9.3ms on the laptop, and a read
// record is decoded and walked past in well under a millisecond.
//
// Ten milliseconds a record therefore fits the slowest measured flip with room,
// and a topic that is mostly reads finishes far inside its budget. A constant
// 30 second deadline failed at about 4000 flips and would have refused to start
// flipr on a log it could have restored.
const (
	replayBase      = 30 * time.Second
	replayPerRecord = 10 * time.Millisecond
	// replayStall bounds silence, not size: a broker that stops answering
	// mid-replay must not consume the whole budget before flipr says so.
	replayStall = 30 * time.Second
	// replayLogEvery is how often progress is logged, in records.
	replayLogEvery = 10000
)

// replayDeadline sizes the deadline from the number of records the broker
// says stand between the start and end offsets.
func replayDeadline(records int64) time.Duration {
	return replayBase + time.Duration(records)*replayPerRecord
}

// replayBounds asks the broker, once, where every partition of the topic
// starts and ends right now. targets holds the end offset per partition;
//
// next holds the first offset replay expects from each, which is the start
// offset rather than zero because retention may have removed early records.
// total is the number of records between the two, summed.
func replayBounds(
	ctx context.Context,
	cl *kgo.Client,
	topic string,
) (targets, next map[int32]int64, total int64, err error) {
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, nil, 0, fmt.Errorf(
			"replay: listing end offsets: %w",
			err,
		)
	}
	if err := ends.Error(); err != nil {
		return nil, nil, 0, fmt.Errorf("replay: end offsets: %w", err)
	}
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, nil, 0, fmt.Errorf(
			"replay: listing start offsets: %w",
			err,
		)
	}
	if err := starts.Error(); err != nil {
		return nil, nil, 0, fmt.Errorf("replay: start offsets: %w", err)
	}
	targets, next = map[int32]int64{}, map[int32]int64{}
	for p, end := range ends[topic] {
		start := starts[topic][p].Offset
		targets[p] = end.Offset
		next[p] = start
		total += end.Offset - start
	}
	if len(targets) == 0 {
		return nil, nil, 0, fmt.Errorf(
			"replay: topic %q has no partitions; was it created?",
			topic,
		)
	}
	return targets, next, total, nil
}

// replayCaughtUp reports whether every partition has been consumed to the
// end offset read at start.
func replayCaughtUp(next, targets map[int32]int64) bool {
	for p, end := range targets {
		if next[p] < end {
			return false
		}
	}
	return true
}

// decodeOperation reads either record generation: framed binary protobuf
// first (the contract), unframed protojson second (the immutable history).
func decodeOperation(raw []byte) (*oplogpb.Operation, error) {
	var op oplogpb.Operation
	if payload, err := consume.StripFrame(raw); err == nil {
		if err := proto.Unmarshal(payload, &op); err == nil {
			return &op, nil
		}
	}
	if err := protojson.Unmarshal(raw, &op); err != nil {
		return nil, fmt.Errorf(
			"neither SR-framed protobuf nor protojson: %w",
			err,
		)
	}
	return &op, nil
}
