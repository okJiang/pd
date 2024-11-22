// Copyright 2020 TiKV Project Authors.
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

package tso

import (
	"context"
	"math"
	"path"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// GlobalDCLocation is the Global TSO Allocator's DC location label.
	GlobalDCLocation            = "global"
	checkStep                   = time.Minute
	patrolStep                  = time.Second
	defaultAllocatorLeaderLease = 3
)

var (
	// PriorityCheck exported is only for test.
	PriorityCheck = time.Minute
)

// AllocatorGroupFilter is used to select AllocatorGroup.
type AllocatorGroupFilter func(ag *allocatorGroup) bool

type allocatorGroup struct {
	// ctx is built with cancel from a parent context when set up which can be different
	// in order to receive Done() signal correctly.
	// cancel would be call when allocatorGroup is deleted to stop background loop.
	ctx    context.Context
	cancel context.CancelFunc
	// For the Global TSO Allocator, leadership is a PD leader's
	// leadership, and for the Local TSO Allocator, leadership
	// is a DC-level certificate to allow an allocator to generate
	// TSO for local transactions in its DC.
	leadership *election.Leadership
	allocator  Allocator
}

// ElectionMember defines the interface for the election related logic.
type ElectionMember interface {
	// ID returns the unique ID in the election group. For example, it can be unique
	// server id of a cluster or the unique keyspace group replica id of the election
	// group composed of the replicas of a keyspace group.
	ID() uint64
	// Name returns the unique name in the election group.
	Name() string
	// MemberValue returns the member value.
	MemberValue() string
	// GetMember returns the current member
	GetMember() any
	// Client returns the etcd client.
	Client() *clientv3.Client
	// IsLeader returns whether the participant is the leader or not by checking its
	// leadership's lease and leader info.
	IsLeader() bool
	// IsLeaderElected returns true if the leader exists; otherwise false.
	IsLeaderElected() bool
	// CheckLeader checks if someone else is taking the leadership. If yes, returns the leader;
	// otherwise returns a bool which indicates if it is needed to check later.
	CheckLeader() (leader member.ElectionLeader, checkAgain bool)
	// EnableLeader declares the member itself to be the leader.
	EnableLeader()
	// KeepLeader is used to keep the leader's leadership.
	KeepLeader(ctx context.Context)
	// CampaignLeader is used to campaign the leadership and make it become a leader in an election group.
	CampaignLeader(ctx context.Context, leaseTimeout int64) error
	// ResetLeader is used to reset the member's current leadership.
	// Basically it will reset the leader lease and unset leader info.
	ResetLeader()
	// GetLeaderListenUrls returns current leader's listen urls
	// The first element is the leader/primary url
	GetLeaderListenUrls() []string
	// GetLeaderID returns current leader's member ID.
	GetLeaderID() uint64
	// GetLeaderPath returns the path of the leader.
	GetLeaderPath() string
	// GetLeadership returns the leadership of the election member.
	GetLeadership() *election.Leadership
	// GetLastLeaderUpdatedTime returns the last time when the leader is updated.
	GetLastLeaderUpdatedTime() time.Time
	// GetDCLocationPathPrefix returns the dc-location path prefix of the cluster.
	GetDCLocationPathPrefix() string
	// GetDCLocationPath returns the dc-location path of a member with the given member ID.
	GetDCLocationPath(id uint64) string
	// PreCheckLeader does some pre-check before checking whether it's the leader.
	PreCheckLeader() error
}

// AllocatorManager is used to manage the TSO Allocators a PD server holds.
// It is in charge of maintaining TSO allocators' leadership, checking election
// priority, and forwarding TSO allocation requests to correct TSO Allocators.
type AllocatorManager struct {
	mu struct {
		syncutil.RWMutex
		allocatorGroup *allocatorGroup
		// The max suffix sign we have so far, it will be used to calculate
		// the number of suffix bits we need in the TSO logical part.
		maxSuffix int32
	}
	// for the synchronization purpose of the allocator update checks
	wg sync.WaitGroup
	// for the synchronization purpose of the service loops
	svcLoopWG sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
	// kgID is the keyspace group ID
	kgID uint32
	// member is for election use
	member ElectionMember
	// TSO config
	rootPath               string
	storage                endpoint.TSOStorage
	saveInterval           time.Duration
	updatePhysicalInterval time.Duration
	// leaderLease defines the time within which a TSO primary/leader must update its TTL
	// in etcd, otherwise etcd will expire the leader key and other servers can campaign
	// the primary/leader again. Etcd only supports seconds TTL, so here is second too.
	leaderLease    int64
	maxResetTSGap  func() time.Duration
	securityConfig *grpcutil.TLSConfig
}

// NewAllocatorManager creates a new TSO Allocator Manager.
func NewAllocatorManager(
	ctx context.Context,
	keyspaceGroupID uint32,
	member ElectionMember,
	rootPath string,
	storage endpoint.TSOStorage,
	cfg Config,
) *AllocatorManager {
	ctx, cancel := context.WithCancel(ctx)
	am := &AllocatorManager{
		ctx:                    ctx,
		cancel:                 cancel,
		kgID:                   keyspaceGroupID,
		member:                 member,
		rootPath:               rootPath,
		storage:                storage,
		saveInterval:           cfg.GetTSOSaveInterval(),
		updatePhysicalInterval: cfg.GetTSOUpdatePhysicalInterval(),
		leaderLease:            cfg.GetLeaderLease(),
		maxResetTSGap:          cfg.GetMaxResetTSGap,
		securityConfig:         cfg.GetTLSConfig(),
	}

	// Set up the Global TSO Allocator here, it will be initialized once the member campaigns leader successfully.
	am.SetUpGlobalAllocator(am.ctx, am.member.GetLeadership())
	am.svcLoopWG.Add(1)

	return am
}

// SetUpGlobalAllocator is used to set up the global allocator, which will initialize the allocator and put it into
// an allocator daemon. An TSO Allocator should only be set once, and may be initialized and reset multiple times
// depending on the election.
func (am *AllocatorManager) SetUpGlobalAllocator(ctx context.Context, leadership *election.Leadership) {
	am.mu.Lock()
	defer am.mu.Unlock()

	allocator := NewGlobalTSOAllocator(ctx, am)
	// Create a new allocatorGroup
	ctx, cancel := context.WithCancel(ctx)
	am.mu.allocatorGroup = &allocatorGroup{
		ctx:        ctx,
		cancel:     cancel,
		leadership: leadership,
		allocator:  allocator,
	}
}

// getGroupID returns the keyspace group ID of the allocator manager.
func (am *AllocatorManager) getGroupID() uint32 {
	if am == nil {
		return 0
	}
	return am.kgID
}

// getGroupIDStr returns the keyspace group ID of the allocator manager in string format.
func (am *AllocatorManager) getGroupIDStr() string {
	if am == nil {
		return "0"
	}
	return strconv.FormatUint(uint64(am.kgID), 10)
}

// GetTimestampPath returns the timestamp path in etcd.
func (am *AllocatorManager) GetTimestampPath() string {
	if am == nil {
		return ""
	}

	am.mu.RLock()
	defer am.mu.RUnlock()
	return path.Join(am.rootPath, am.mu.allocatorGroup.allocator.GetTimestampPath())
}

// close is used to shutdown TSO Allocator updating daemon.
// tso service call this function to shutdown the loop here, but pd manages its own loop.
func (am *AllocatorManager) close() {
	log.Info("closing the allocator manager", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))

	am.mu.allocatorGroup.allocator.(*GlobalTSOAllocator).close()

	am.cancel()
	am.svcLoopWG.Wait()

	log.Info("closed the allocator manager", logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0))
}

// GetMember returns the ElectionMember of this AllocatorManager.
func (am *AllocatorManager) GetMember() ElectionMember {
	return am.member
}

// GetClusterDCLocationsFromEtcd fetches dcLocation topology from etcd
func (am *AllocatorManager) GetClusterDCLocationsFromEtcd() (clusterDCLocations map[string][]uint64, err error) {
	resp, err := etcdutil.EtcdKVGet(
		am.member.Client(),
		am.member.GetDCLocationPathPrefix(),
		clientv3.WithPrefix())
	if err != nil {
		return clusterDCLocations, err
	}
	clusterDCLocations = make(map[string][]uint64)
	for _, kv := range resp.Kvs {
		// The key will contain the member ID and the value is its dcLocation
		serverPath := strings.Split(string(kv.Key), "/")
		// Get serverID from serverPath, e.g, /pd/dc-location/1232143243253 -> 1232143243253
		serverID, err := strconv.ParseUint(serverPath[len(serverPath)-1], 10, 64)
		dcLocation := string(kv.Value)
		if err != nil {
			log.Warn("get server id and dcLocation from etcd failed, invalid server id",
				logutil.CondUint32("keyspace-group-id", am.kgID, am.kgID > 0),
				zap.Any("split-serverPath", serverPath),
				zap.String("dc-location", dcLocation),
				errs.ZapError(err))
			continue
		}
		clusterDCLocations[dcLocation] = append(clusterDCLocations[dcLocation], serverID)
	}
	return clusterDCLocations, nil
}

// compareAndSetMaxSuffix sets the max suffix sign if suffix is greater than am.mu.maxSuffix.
func (am *AllocatorManager) compareAndSetMaxSuffix(suffix int32) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if suffix > am.mu.maxSuffix {
		am.mu.maxSuffix = suffix
	}
}

// GetSuffixBits calculates the bits of suffix sign
// by the max number of suffix so far,
// which will be used in the TSO logical part.
func (am *AllocatorManager) GetSuffixBits() int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return CalSuffixBits(am.mu.maxSuffix)
}

// CalSuffixBits calculates the bits of suffix by the max suffix sign.
func CalSuffixBits(maxSuffix int32) int {
	// maxSuffix + 1 because we have the Global TSO holds 0 as the suffix sign
	return int(math.Ceil(math.Log2(float64(maxSuffix + 1))))
}

// longSleep is used to sleep the long wait duration while also watching the
// ctx.Done() to prevent the goroutine from leaking. This function returns
// true if the sleep is over, false if the ctx is done.
func longSleep(ctx context.Context, waitStep time.Duration) bool {
	waitTicker := time.NewTicker(waitStep)
	defer waitTicker.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-waitTicker.C:
		return true
	}
}

// HandleRequest forwards TSO allocation requests to correct TSO Allocators.
func (am *AllocatorManager) HandleRequest(ctx context.Context, count uint32) (pdpb.Timestamp, error) {
	defer trace.StartRegion(ctx, "AllocatorManager.HandleRequest").End()
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.mu.allocatorGroup.allocator.GenerateTSO(ctx, count)
}

// ResetAllocatorGroup will reset the allocator's leadership and TSO initialized in memory.
// It usually should be called before re-triggering an Allocator leader campaign.
func (am *AllocatorManager) ResetAllocatorGroup(skipResetLeader bool) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.mu.allocatorGroup.allocator.Reset()
	// Reset if it still has the leadership. Otherwise the data race may occur because of the re-campaigning.
	if !skipResetLeader && am.mu.allocatorGroup.leadership.Check() {
		am.mu.allocatorGroup.leadership.Reset()
	}
}

// IsLeader returns whether the current member is the leader in the election group.
func (am *AllocatorManager) IsLeader() bool {
	if am == nil || am.member == nil || !am.member.IsLeader() {
		return false
	}
	return true
}

// GetLeaderAddr returns the address of leader in the election group.
func (am *AllocatorManager) GetLeaderAddr() string {
	if am == nil || am.member == nil {
		return ""
	}
	leaderAddrs := am.member.GetLeaderListenUrls()
	if len(leaderAddrs) < 1 {
		return ""
	}
	return leaderAddrs[0]
}

func (am *AllocatorManager) startGlobalAllocatorLoop() {
	globalTSOAllocator, ok := am.mu.allocatorGroup.allocator.(*GlobalTSOAllocator)
	if !ok {
		// it should never happen
		log.Error("failed to start global allocator loop, global allocator not found")
		return
	}
	globalTSOAllocator.wg.Add(1)
	go globalTSOAllocator.primaryElectionLoop()
}
