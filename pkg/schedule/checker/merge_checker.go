// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checker

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	maxTargetRegionSize   = 500
	maxTargetRegionFactor = 4
)

// When a region has label `merge_option=deny`, skip merging the region.
// If label value is `allow` or other value, it will be treated as `allow`.
const (
	mergeOptionLabel     = "merge_option"
	mergeOptionValueDeny = "deny"
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	mergeCheckerCounter                     = counterWithEvent(config.MergeCheckerName, "check")
	mergeCheckerPausedCounter               = counterWithEvent(config.MergeCheckerName, "paused")
	mergeCheckerRecentlySplitCounter        = counterWithEvent(config.MergeCheckerName, "recently-split")
	mergeCheckerRecentlyStartCounter        = counterWithEvent(config.MergeCheckerName, "recently-start")
	mergeCheckerNoLeaderCounter             = counterWithEvent(config.MergeCheckerName, "no-leader")
	mergeCheckerNoNeedCounter               = counterWithEvent(config.MergeCheckerName, "no-need")
	mergeCheckerUnhealthyRegionCounter      = counterWithEvent(config.MergeCheckerName, "unhealthy-region")
	mergeCheckerAbnormalReplicaCounter      = counterWithEvent(config.MergeCheckerName, "abnormal-replica")
	mergeCheckerHotRegionCounter            = counterWithEvent(config.MergeCheckerName, "hot-region")
	mergeCheckerNoTargetCounter             = counterWithEvent(config.MergeCheckerName, "no-target")
	mergeCheckerTargetTooLargeCounter       = counterWithEvent(config.MergeCheckerName, "target-too-large")
	mergeCheckerSplitSizeAfterMergeCounter  = counterWithEvent(config.MergeCheckerName, "split-size-after-merge")
	mergeCheckerSplitKeysAfterMergeCounter  = counterWithEvent(config.MergeCheckerName, "split-keys-after-merge")
	mergeCheckerNewOpCounter                = counterWithEvent(config.MergeCheckerName, "new-operator")
	mergeCheckerLargerSourceCounter         = counterWithEvent(config.MergeCheckerName, "larger-source")
	mergeCheckerAdjNotExistCounter          = counterWithEvent(config.MergeCheckerName, "adj-not-exist")
	mergeCheckerAdjRecentlySplitCounter     = counterWithEvent(config.MergeCheckerName, "adj-recently-split")
	mergeCheckerAdjRegionHotCounter         = counterWithEvent(config.MergeCheckerName, "adj-region-hot")
	mergeCheckerAdjDisallowMergeCounter     = counterWithEvent(config.MergeCheckerName, "adj-disallow-merge")
	mergeCheckerAdjAbnormalPeerStoreCounter = counterWithEvent(config.MergeCheckerName, "adj-abnormal-peerstore")
	mergeCheckerAdjSpecialPeerCounter       = counterWithEvent(config.MergeCheckerName, "adj-special-peer")
	mergeCheckerAdjAbnormalReplicaCounter   = counterWithEvent(config.MergeCheckerName, "adj-abnormal-replica")
)

// MergeChecker ensures region to merge with adjacent region when size is small
type MergeChecker struct {
	PauseController
	cluster    sche.CheckerCluster
	conf       config.CheckerConfigProvider
	splitCache *cache.TTLUint64
	startTime  time.Time // it's used to judge whether server recently start.
}

// NewMergeChecker creates a merge checker.
func NewMergeChecker(ctx context.Context, cluster sche.CheckerCluster, conf config.CheckerConfigProvider) *MergeChecker {
	splitCache := cache.NewIDTTL(ctx, time.Minute, conf.GetSplitMergeInterval())
	return &MergeChecker{
		cluster:    cluster,
		conf:       conf,
		splitCache: splitCache,
		startTime:  time.Now(),
	}
}

// Name returns the checker name.
func (*MergeChecker) Name() string {
	return config.MergeCheckerName.String()
}

// RecordRegionSplit put the recently split region into cache. MergeChecker
// will skip check it for a while.
func (m *MergeChecker) RecordRegionSplit(regionIDs []uint64) {
	for _, regionID := range regionIDs {
		m.splitCache.PutWithTTL(regionID, nil, m.conf.GetSplitMergeInterval())
	}
}

// Check verifies a region's replicas, creating an Operator if need.
func (m *MergeChecker) Check(region *core.RegionInfo) []*operator.Operator {
	mergeCheckerCounter.Inc()

	if m.IsPaused() {
		mergeCheckerPausedCounter.Inc()
		return nil
	}

	expireTime := m.startTime.Add(m.conf.GetSplitMergeInterval())
	if time.Now().Before(expireTime) {
		mergeCheckerRecentlyStartCounter.Inc()
		return nil
	}

	m.splitCache.UpdateTTL(m.conf.GetSplitMergeInterval())
	if m.splitCache.Exists(region.GetID()) {
		mergeCheckerRecentlySplitCounter.Inc()
		return nil
	}

	// when pd just started, it will load region meta from region storage,
	if region.GetLeader() == nil {
		mergeCheckerNoLeaderCounter.Inc()
		return nil
	}

	// region is not small enough
	if !region.NeedMerge(int64(m.conf.GetMaxMergeRegionSize()), int64(m.conf.GetMaxMergeRegionKeys())) {
		mergeCheckerNoNeedCounter.Inc()
		return nil
	}

	// skip region has down peers or pending peers
	if !filter.IsRegionHealthy(region) {
		mergeCheckerUnhealthyRegionCounter.Inc()
		return nil
	}

	if !filter.IsRegionReplicated(m.cluster, region) {
		mergeCheckerAbnormalReplicaCounter.Inc()
		return nil
	}

	// skip hot region
	if m.cluster.IsRegionHot(region) {
		mergeCheckerHotRegionCounter.Inc()
		return nil
	}

	prev, next := m.cluster.GetAdjacentRegions(region)

	var target *core.RegionInfo
	if m.checkTarget(region, next) {
		target = next
	}
	if !m.conf.IsOneWayMergeEnabled() && m.checkTarget(region, prev) { // allow a region can be merged by two ways.
		if target == nil || prev.GetApproximateSize() < next.GetApproximateSize() { // pick smaller
			target = prev
		}
	}

	if target == nil {
		mergeCheckerNoTargetCounter.Inc()
		return nil
	}

	regionMaxSize := m.cluster.GetStoreConfig().GetRegionMaxSize()
	maxTargetRegionSizeThreshold := int64(float64(regionMaxSize) * float64(maxTargetRegionFactor))
	if maxTargetRegionSizeThreshold < maxTargetRegionSize {
		maxTargetRegionSizeThreshold = maxTargetRegionSize
	}
	if target.GetApproximateSize() > maxTargetRegionSizeThreshold {
		mergeCheckerTargetTooLargeCounter.Inc()
		return nil
	}
	if err := m.cluster.GetStoreConfig().CheckRegionSize(uint64(target.GetApproximateSize()+region.GetApproximateSize()),
		m.conf.GetMaxMergeRegionSize()); err != nil {
		mergeCheckerSplitSizeAfterMergeCounter.Inc()
		return nil
	}

	if err := m.cluster.GetStoreConfig().CheckRegionKeys(uint64(target.GetApproximateKeys()+region.GetApproximateKeys()),
		m.conf.GetMaxMergeRegionKeys()); err != nil {
		mergeCheckerSplitKeysAfterMergeCounter.Inc()
		return nil
	}

	log.Debug("try to merge region",
		logutil.ZapRedactStringer("from", core.RegionToHexMeta(region.GetMeta())),
		logutil.ZapRedactStringer("to", core.RegionToHexMeta(target.GetMeta())))
	ops, err := operator.CreateMergeRegionOperator("merge-region", m.cluster, region, target, operator.OpMerge)
	if err != nil {
		log.Warn("create merge region operator failed", errs.ZapError(err))
		return nil
	}
	mergeCheckerNewOpCounter.Inc()
	if region.GetApproximateSize() > target.GetApproximateSize() ||
		region.GetApproximateKeys() > target.GetApproximateKeys() {
		mergeCheckerLargerSourceCounter.Inc()
	}
	return ops
}

func (m *MergeChecker) checkTarget(region, adjacent *core.RegionInfo) bool {
	if adjacent == nil {
		mergeCheckerAdjNotExistCounter.Inc()
		return false
	}

	if m.splitCache.Exists(adjacent.GetID()) {
		mergeCheckerAdjRecentlySplitCounter.Inc()
		return false
	}

	if m.cluster.IsRegionHot(adjacent) {
		mergeCheckerAdjRegionHotCounter.Inc()
		return false
	}

	if !AllowMerge(m.cluster, region, adjacent) {
		mergeCheckerAdjDisallowMergeCounter.Inc()
		return false
	}

	if !checkPeerStore(m.cluster, region, adjacent) {
		mergeCheckerAdjAbnormalPeerStoreCounter.Inc()
		return false
	}

	if !filter.IsRegionHealthy(adjacent) {
		mergeCheckerAdjSpecialPeerCounter.Inc()
		return false
	}

	if !filter.IsRegionReplicated(m.cluster, adjacent) {
		mergeCheckerAdjAbnormalReplicaCounter.Inc()
		return false
	}

	return true
}

// AllowMerge returns true if two regions can be merged according to the key type.
func AllowMerge(cluster sche.SharedCluster, region, adjacent *core.RegionInfo) bool {
	var start, end []byte
	if bytes.Equal(region.GetEndKey(), adjacent.GetStartKey()) && len(region.GetEndKey()) != 0 {
		start, end = region.GetStartKey(), adjacent.GetEndKey()
	} else if bytes.Equal(adjacent.GetEndKey(), region.GetStartKey()) && len(adjacent.GetEndKey()) != 0 {
		start, end = adjacent.GetStartKey(), region.GetEndKey()
	} else {
		return false
	}

	// The interface probe is used here to get the rule manager and region
	// labeler because AllowMerge is also used by the random merge scheduler,
	// where it is not easy to get references to concrete objects.
	// We can consider using dependency injection techniques to optimize in
	// the future.

	if cluster.GetSharedConfig().IsPlacementRulesEnabled() {
		cl, ok := cluster.(interface{ GetRuleManager() *placement.RuleManager })
		if !ok || len(cl.GetRuleManager().GetSplitKeys(start, end)) > 0 {
			return false
		}
	}

	if cl, ok := cluster.(interface{ GetRegionLabeler() *labeler.RegionLabeler }); ok {
		l := cl.GetRegionLabeler()
		if len(l.GetSplitKeys(start, end)) > 0 {
			return false
		}
		if l.GetRegionLabel(region, mergeOptionLabel) == mergeOptionValueDeny || l.GetRegionLabel(adjacent, mergeOptionLabel) == mergeOptionValueDeny {
			return false
		}
	}

	policy := cluster.GetSharedConfig().GetKeyType()
	switch policy {
	case constant.Table:
		if cluster.GetSharedConfig().IsCrossTableMergeEnabled() {
			return true
		}
		return isTableIDSame(region, adjacent)
	case constant.Raw:
		return true
	case constant.Txn:
		return true
	default:
		return isTableIDSame(region, adjacent)
	}
}

func isTableIDSame(region, adjacent *core.RegionInfo) bool {
	return codec.Key(region.GetStartKey()).TableID() == codec.Key(adjacent.GetStartKey()).TableID()
}

// Check whether there is a peer of the adjacent region on an offline store,
// while the source region has no peer on it. This is to prevent from bringing
// any other peer into an offline store to slow down the offline process.
func checkPeerStore(cluster sche.SharedCluster, region, adjacent *core.RegionInfo) bool {
	regionStoreIDs := region.GetStoreIDs()
	for _, peer := range adjacent.GetPeers() {
		storeID := peer.GetStoreId()
		store := cluster.GetStore(storeID)
		if store == nil || store.IsRemoving() {
			if _, ok := regionStoreIDs[storeID]; !ok {
				return false
			}
		}
	}
	return true
}
