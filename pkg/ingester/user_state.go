package ingester

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/segmentio/fasthash/fnv1a"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ingester/index"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/extract"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// userStates holds the userState object for all users (tenants),
// each one containing all the in-memory series for a given user.
type userStates struct {
	states  sync.Map
	limiter *SeriesLimiter
	cfg     Config
	metrics *ingesterMetrics
}

type userState struct {
	limiter             *SeriesLimiter
	userID              string
	fpLocker            *fingerprintLocker
	fpToSeries          *seriesMap
	mapper              *fpMapper
	index               *index.InvertedIndex
	ingestedAPISamples  *ewmaRate
	ingestedRuleSamples *ewmaRate

	seriesInMetric []metricCounterShard

	memSeries             prometheus.Gauge
	memSeriesCreatedTotal prometheus.Counter
	memSeriesRemovedTotal prometheus.Counter
	discardedSamples      *prometheus.CounterVec
	createdChunks         prometheus.Counter
}

const metricCounterShards = 128

// DiscardedSamples metric labels
const (
	perUserSeriesLimit   = "per_user_series_limit"
	perMetricSeriesLimit = "per_metric_series_limit"
)

type metricCounterShard struct {
	mtx sync.Mutex
	m   map[string]int
}

func newUserStates(limiter *SeriesLimiter, cfg Config, metrics *ingesterMetrics) *userStates {
	return &userStates{
		limiter: limiter,
		cfg:     cfg,
		metrics: metrics,
	}
}

func (us *userStates) cp() map[string]*userState {
	states := map[string]*userState{}
	us.states.Range(func(key, value interface{}) bool {
		states[key.(string)] = value.(*userState)
		return true
	})
	return states
}

//nolint:unused
func (us *userStates) gc() {
	us.states.Range(func(key, value interface{}) bool {
		state := value.(*userState)
		if state.fpToSeries.length() == 0 {
			us.states.Delete(key)
		}
		return true
	})
}

func (us *userStates) updateRates() {
	us.states.Range(func(key, value interface{}) bool {
		state := value.(*userState)
		state.ingestedAPISamples.tick()
		state.ingestedRuleSamples.tick()
		return true
	})
}

func (us *userStates) get(userID string) (*userState, bool) {
	state, ok := us.states.Load(userID)
	if !ok {
		return nil, ok
	}
	return state.(*userState), ok
}

func (us *userStates) getOrCreate(userID string) *userState {
	state, ok := us.get(userID)
	if !ok {

		seriesInMetric := make([]metricCounterShard, 0, metricCounterShards)
		for i := 0; i < metricCounterShards; i++ {
			seriesInMetric = append(seriesInMetric, metricCounterShard{
				m: map[string]int{},
			})
		}

		// Speculatively create a userState object and try to store it
		// in the map.  Another goroutine may have got there before
		// us, in which case this userState will be discarded
		state = &userState{
			userID:              userID,
			limiter:             us.limiter,
			fpToSeries:          newSeriesMap(),
			fpLocker:            newFingerprintLocker(16 * 1024),
			index:               index.New(),
			ingestedAPISamples:  newEWMARate(0.2, us.cfg.RateUpdatePeriod),
			ingestedRuleSamples: newEWMARate(0.2, us.cfg.RateUpdatePeriod),
			seriesInMetric:      seriesInMetric,

			memSeries:             us.metrics.memSeries,
			memSeriesCreatedTotal: us.metrics.memSeriesCreatedTotal.WithLabelValues(userID),
			memSeriesRemovedTotal: us.metrics.memSeriesRemovedTotal.WithLabelValues(userID),
			discardedSamples:      validation.DiscardedSamples.MustCurryWith(prometheus.Labels{"user": userID}),
			createdChunks:         us.metrics.createdChunks,
		}
		state.mapper = newFPMapper(state.fpToSeries)
		stored, ok := us.states.LoadOrStore(userID, state)
		if !ok {
			us.metrics.memUsers.Inc()
		}
		state = stored.(*userState)
	}

	return state
}

func (us *userStates) getViaContext(ctx context.Context) (*userState, bool, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("no user id")
	}
	state, ok := us.get(userID)
	return state, ok, nil
}

// NOTE: memory for `labels` is unsafe; anything retained beyond the
// life of this function must be copied
func (us *userStates) getOrCreateSeries(ctx context.Context, userID string, labels []client.LabelAdapter, record *Record) (*userState, model.Fingerprint, *memorySeries, error) {
	state := us.getOrCreate(userID)
	// WARNING: `err` may have a reference to unsafe memory in `labels`
	fp, series, err := state.getSeries(labels, record)
	return state, fp, series, err
}

// NOTE: memory for `metric` is unsafe; anything retained beyond the
// life of this function must be copied
func (u *userState) getSeries(metric labelPairs, record *Record) (model.Fingerprint, *memorySeries, error) {
	rawFP := client.FastFingerprint(metric)
	u.fpLocker.Lock(rawFP)
	fp := u.mapper.mapFP(rawFP, metric)
	if fp != rawFP {
		u.fpLocker.Unlock(rawFP)
		u.fpLocker.Lock(fp)
	}

	series, ok := u.fpToSeries.get(fp)
	if ok {
		return fp, series, nil
	}

	series, err := u.createSeriesWithFingerprint(fp, metric, record, false)
	if err != nil {
		u.fpLocker.Unlock(fp)
		return 0, nil, err
	}

	return fp, series, nil
}

func (u *userState) createSeriesWithFingerprint(fp model.Fingerprint, metric labelPairs, record *Record, recovery bool) (*memorySeries, error) {
	// There's theoretically a relatively harmless race here if multiple
	// goroutines get the length of the series map at the same time, then
	// all proceed to add a new series. This is likely not worth addressing,
	// as this should happen rarely (all samples from one push are added
	// serially), and the overshoot in allowed series would be minimal.

	if !recovery {
		if err := u.limiter.AssertMaxSeriesPerUser(u.userID, u.fpToSeries.length()); err != nil {
			return nil, makeLimitError(perUserSeriesLimit, err)
		}
	}

	// MetricNameFromLabelAdapters returns a copy of the string in `metric`
	metricName, err := extract.MetricNameFromLabelAdapters(metric)
	if err != nil {
		return nil, err
	}

	if !recovery {
		// Check if the per-metric limit has been exceeded
		if err = u.canAddSeriesFor(string(metricName)); err != nil {
			// WARNING: returns a reference to `metric`
			return nil, makeMetricLimitError(perMetricSeriesLimit, client.FromLabelAdaptersToLabels(metric), err)
		}
	}

	u.memSeriesCreatedTotal.Inc()
	u.memSeries.Inc()

	if record != nil {
		record.Labels = append(record.Labels, Labels{
			Fingerprint: uint64(fp),
			Labels:      metric,
		})
	}

	labels := u.index.Add(metric, fp) // Add() returns 'interned' values so the original labels are not retained
	series := newMemorySeries(labels, u.createdChunks)
	u.fpToSeries.put(fp, series)

	return series, nil
}

func (u *userState) canAddSeriesFor(metric string) error {
	shard := &u.seriesInMetric[util.HashFP(model.Fingerprint(fnv1a.HashString64(string(metric))))%metricCounterShards]
	shard.mtx.Lock()
	defer shard.mtx.Unlock()

	err := u.limiter.AssertMaxSeriesPerMetric(u.userID, shard.m[metric])
	if err != nil {
		return err
	}

	shard.m[metric]++
	return nil
}

func (u *userState) removeSeries(fp model.Fingerprint, metric labels.Labels) {
	u.fpToSeries.del(fp)
	u.index.Delete(labels.Labels(metric), fp)

	metricName := metric.Get(model.MetricNameLabel)
	if metricName == "" {
		// Series without a metric name should never be able to make it into
		// the ingester's memory storage.
		panic("No metric name label")
	}

	shard := &u.seriesInMetric[util.HashFP(model.Fingerprint(fnv1a.HashString64(string(metricName))))%metricCounterShards]
	shard.mtx.Lock()
	defer shard.mtx.Unlock()

	shard.m[metricName]--
	if shard.m[metricName] == 0 {
		delete(shard.m, metricName)
	}

	u.memSeriesRemovedTotal.Inc()
	u.memSeries.Dec()
}

// forSeriesMatching passes all series matching the given matchers to the
// provided callback. Deals with locking and the quirks of zero-length matcher
// values. There are 2 callbacks:
// - The `add` callback is called for each series while the lock is held, and
//   is intend to be used by the caller to build a batch.
// - The `send` callback is called at certain intervals specified by batchSize
//   with no locks held, and is intended to be used by the caller to send the
//   built batches.
func (u *userState) forSeriesMatching(ctx context.Context, allMatchers []*labels.Matcher,
	add func(context.Context, model.Fingerprint, *memorySeries) error,
	send func(context.Context) error, batchSize int,
) error {
	log, ctx := spanlogger.New(ctx, "forSeriesMatching")
	defer log.Finish()

	filters, matchers := util.SplitFiltersAndMatchers(allMatchers)
	fps := u.index.Lookup(matchers)
	if len(fps) > u.limiter.MaxSeriesPerQuery(u.userID) {
		return httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "exceeded maximum number of series in a query")
	}

	level.Debug(log).Log("series", len(fps))

	// We only hold one FP lock at once here, so no opportunity to deadlock.
	sent := 0
outer:
	for _, fp := range fps {
		if err := ctx.Err(); err != nil {
			return err
		}

		u.fpLocker.Lock(fp)
		series, ok := u.fpToSeries.get(fp)
		if !ok {
			u.fpLocker.Unlock(fp)
			continue
		}

		for _, filter := range filters {
			if !filter.Matches(series.metric.Get(filter.Name)) {
				u.fpLocker.Unlock(fp)
				continue outer
			}
		}

		err := add(ctx, fp, series)
		u.fpLocker.Unlock(fp)
		if err != nil {
			return err
		}

		sent++
		if batchSize > 0 && sent%batchSize == 0 && send != nil {
			if err = send(ctx); err != nil {
				return nil
			}
		}
	}

	if batchSize > 0 && sent%batchSize > 0 && send != nil {
		return send(ctx)
	}
	return nil
}
