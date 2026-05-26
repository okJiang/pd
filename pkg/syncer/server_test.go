// Copyright 2026 TiKV Project Authors.
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

package syncer

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockserver"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestHistoryBufferSizeFromMemory(t *testing.T) {
	testCases := []struct {
		name        string
		totalMemory uint64
		expected    int
	}{
		{name: "zero-memory", totalMemory: 0, expected: defaultHistoryBufferSize},
		{name: "below-minimum", totalMemory: historyBufferMemoryStep / 2, expected: defaultHistoryBufferSize},
		{name: "base-step", totalMemory: historyBufferMemoryStep, expected: defaultHistoryBufferSize},
		{name: "round-to-two-units", totalMemory: historyBufferMemoryStep * 3 / 2, expected: 2 * defaultHistoryBufferSize},
		{name: "max-clamped", totalMemory: historyBufferMemoryStep * 64 / 4, expected: maxHistoryBufferBaseSize},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, historyBufferSizeFromMemory(testCase.totalMemory))
		})
	}
}

func TestSyncExitsWhenRegionSyncerStops(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer func() {
		re.NoError(regionStorage.Close())
	}()

	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), regionStorage),
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	ctx, cancel := context.WithCancel(context.Background())
	stream := newMockSyncRegionsServer()
	done := make(chan error, 1)
	go func() {
		done <- syncer.Sync(ctx, stream)
	}()

	stream.recvCh <- &pdpb.SyncRegionRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member: &pdpb.Member{
			Name:       "pd-follower",
			ClientUrls: []string{"http://127.0.0.1:2379"},
		},
	}
	re.NotNil(<-stream.sendCh)
	re.NotNil(<-stream.sendCh)
	testutil.Eventually(re, func() bool {
		names := syncer.GetAllDownstreamNames()
		return len(names) == 1 && names[0] == "pd-follower"
	})

	cancel()
	var syncErr error
	testutil.Eventually(re, func() bool {
		if syncErr == nil {
			select {
			case syncErr = <-done:
			default:
				return false
			}
		}
		st, ok := status.FromError(syncErr)
		return ok && st.Code() == codes.Unavailable
	})
	re.Empty(syncer.GetAllDownstreamNames())
}

func TestSyncExitsWhenBroadcastSendFails(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer func() {
		re.NoError(regionStorage.Close())
	}()

	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), regionStorage),
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newMockSyncRegionsServer()
	done := make(chan error, 1)
	go func() {
		done <- syncer.Sync(ctx, stream)
	}()

	stream.recvCh <- &pdpb.SyncRegionRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member: &pdpb.Member{
			Name:       "pd-follower",
			ClientUrls: []string{"http://127.0.0.1:2379"},
		},
	}
	re.NotNil(<-stream.sendCh)
	re.NotNil(<-stream.sendCh)
	testutil.Eventually(re, func() bool {
		names := syncer.GetAllDownstreamNames()
		return len(names) == 1 && names[0] == "pd-follower"
	})

	stream.setSendErr(errors.New("send failed"))
	syncer.broadcast(context.Background(), &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		StartIndex: syncer.history.getNextIndex(),
	})

	var syncErr error
	testutil.Eventually(re, func() bool {
		if syncErr == nil {
			select {
			case syncErr = <-done:
			default:
				return false
			}
		}
		st, ok := status.FromError(syncErr)
		return ok && st.Code() == codes.Unavailable
	})
	re.Empty(syncer.GetAllDownstreamNames())
}

func TestCloseAllClientClosesStreamsBeforeSend(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer func() {
		re.NoError(regionStorage.Close())
	}()

	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), regionStorage),
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newMockSyncRegionsServer()
	done := make(chan error, 1)
	go func() {
		done <- syncer.Sync(ctx, stream)
	}()

	stream.recvCh <- &pdpb.SyncRegionRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member: &pdpb.Member{
			Name:       "pd-follower",
			ClientUrls: []string{"http://127.0.0.1:2379"},
		},
	}
	re.NotNil(<-stream.sendCh)
	re.NotNil(<-stream.sendCh)
	testutil.Eventually(re, func() bool {
		names := syncer.GetAllDownstreamNames()
		return len(names) == 1 && names[0] == "pd-follower"
	})

	unblockSend := stream.blockSend()
	closeDone := make(chan struct{})
	go func() {
		syncer.closeAllClient()
		close(closeDone)
	}()
	testutil.Eventually(re, stream.isSendBlocked)

	var syncErr error
	testutil.Eventually(re, func() bool {
		if syncErr == nil {
			select {
			case syncErr = <-done:
			default:
				return false
			}
		}
		st, ok := status.FromError(syncErr)
		return ok && st.Code() == codes.Unavailable
	})
	re.Empty(syncer.GetAllDownstreamNames())

	close(unblockSend)
	testutil.Eventually(re, func() bool {
		select {
		case <-closeDone:
			return true
		default:
			return false
		}
	})
}

func TestBroadcastClosesStreamWhenSendBlocks(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer func() {
		re.NoError(regionStorage.Close())
	}()

	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), regionStorage),
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	syncer.sendTimeout = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newMockSyncRegionsServer()
	done := make(chan error, 1)
	go func() {
		done <- syncer.Sync(ctx, stream)
	}()

	stream.recvCh <- &pdpb.SyncRegionRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member: &pdpb.Member{
			Name:       "pd-follower",
			ClientUrls: []string{"http://127.0.0.1:2379"},
		},
	}
	re.NotNil(<-stream.sendCh)
	re.NotNil(<-stream.sendCh)
	testutil.Eventually(re, func() bool {
		names := syncer.GetAllDownstreamNames()
		return len(names) == 1 && names[0] == "pd-follower"
	})

	unblockSend := stream.blockSend()
	broadcastDone := make(chan struct{})
	go func() {
		syncer.broadcast(context.Background(), &pdpb.SyncRegionResponse{
			Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
			StartIndex: syncer.history.getNextIndex(),
		})
		close(broadcastDone)
	}()
	testutil.Eventually(re, stream.isSendBlocked)
	testutil.Eventually(re, func() bool {
		select {
		case <-broadcastDone:
			return true
		default:
			return false
		}
	})

	var syncErr error
	testutil.Eventually(re, func() bool {
		if syncErr == nil {
			select {
			case syncErr = <-done:
			default:
				return false
			}
		}
		st, ok := status.FromError(syncErr)
		return ok && st.Code() == codes.Unavailable
	})
	re.Empty(syncer.GetAllDownstreamNames())
	close(unblockSend)
}

func TestSyncExitsWhenContextCanceledBeforeRequest(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer func() {
		re.NoError(regionStorage.Close())
	}()

	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), regionStorage),
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	ctx, cancel := context.WithCancel(context.Background())
	stream := newMockSyncRegionsServer()
	defer stream.cancel()
	done := make(chan error, 1)
	go func() {
		done <- syncer.Sync(ctx, stream)
	}()

	cancel()
	var syncErr error
	testutil.Eventually(re, func() bool {
		if syncErr == nil {
			select {
			case syncErr = <-done:
			default:
				return false
			}
		}
		st, ok := status.FromError(syncErr)
		return ok && st.Code() == codes.Unavailable
	})
}

func TestSyncFallsBackToFullSyncWhenHistoryMissing(t *testing.T) {
	re := require.New(t)
	syncer, _ := newTestRegionSyncer(t, newTestSyncRegion(1, 11))
	syncer.history.resetWithIndex(100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newMockSyncRegionsServer()
	blockCh := stream.blockSend()
	done := startTestRegionSync(ctx, syncer, stream)

	sendTestSyncRegionRequest(stream, 1)
	testutil.Eventually(re, stream.isSendBlocked)
	syncer.history.record(newTestSyncRegion(2, 12))
	close(blockCh)

	resp := mustRecvSyncRegionResponse(t, stream, "expected full sync response")
	re.Equal(uint64(0), resp.GetStartIndex())
	re.Len(resp.GetRegions(), 1)
	re.Equal(uint64(1), resp.GetRegions()[0].GetId())

	resp = mustRecvSyncRegionResponse(t, stream, "expected full sync catch-up response")
	re.Equal(uint64(100), resp.GetStartIndex())
	re.Len(resp.GetRegions(), 1)
	re.Equal(uint64(2), resp.GetRegions()[0].GetId())

	resp = mustRecvSyncRegionResponse(t, stream, "expected full sync completion response")
	re.Equal(uint64(101), resp.GetStartIndex())
	re.Empty(resp.GetRegions())
	waitTestRegionSyncerBound(re, syncer)

	cancel()
	waitTestRegionSyncerUnavailable(re, done)
}

func TestFullSyncGrowsHistoryBufferDuringCatchUp(t *testing.T) {
	re := require.New(t)
	syncer, bc := newTestRegionSyncer(t, newTestSyncRegion(1, 11))
	syncer.history = newHistoryBufferWithConfig(1, 4, 1, syncer.history.kv)
	syncer.history.resetWithIndex(100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newMockSyncRegionsServer()
	blockCh := stream.blockSend()
	done := startTestRegionSync(ctx, syncer, stream)

	sendTestSyncRegionRequest(stream, 1)
	testutil.Eventually(re, stream.isSendBlocked)
	for _, region := range []*core.RegionInfo{
		newTestSyncRegion(2, 12),
		newTestSyncRegion(3, 13),
	} {
		bc.PutRegion(region)
		syncer.history.record(region)
	}
	close(blockCh)

	resp := mustRecvSyncRegionResponse(t, stream, "expected original full sync response")
	re.Equal(uint64(0), resp.GetStartIndex())
	re.Len(resp.GetRegions(), 1)
	re.Equal(uint64(1), resp.GetRegions()[0].GetId())

	resp = mustRecvSyncRegionResponse(t, stream, "expected full sync catch-up response")
	re.Equal(uint64(100), resp.GetStartIndex())
	re.Len(resp.GetRegions(), 2)
	regionIDs := make([]uint64, 0, len(resp.GetRegions()))
	for _, region := range resp.GetRegions() {
		regionIDs = append(regionIDs, region.GetId())
	}
	re.ElementsMatch([]uint64{2, 3}, regionIDs)

	resp = mustRecvSyncRegionResponse(t, stream, "expected full sync completion response")
	re.Equal(uint64(102), resp.GetStartIndex())
	re.Empty(resp.GetRegions())
	re.Equal(4, syncer.history.capacity())
	waitTestRegionSyncerBound(re, syncer)

	cancel()
	waitTestRegionSyncerUnavailable(re, done)
}

func TestFullSyncFailsWhenRetainedHistoryExceedsMaxCapacity(t *testing.T) {
	re := require.New(t)
	syncer, bc := newTestRegionSyncer(t, newTestSyncRegion(1, 11))
	syncer.history = newHistoryBufferWithConfig(1, 1, 1, syncer.history.kv)
	syncer.history.resetWithIndex(100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newMockSyncRegionsServer()
	blockCh := stream.blockSend()
	done := startTestRegionSync(ctx, syncer, stream)

	sendTestSyncRegionRequest(stream, 1)
	testutil.Eventually(re, stream.isSendBlocked)
	for _, region := range []*core.RegionInfo{
		newTestSyncRegion(2, 12),
		newTestSyncRegion(3, 13),
	} {
		bc.PutRegion(region)
		syncer.history.record(region)
	}
	close(blockCh)

	resp := mustRecvSyncRegionResponse(t, stream, "expected original full sync response")
	re.Equal(uint64(0), resp.GetStartIndex())
	re.Len(resp.GetRegions(), 1)
	re.Equal(uint64(1), resp.GetRegions()[0].GetId())

	var syncErr error
	testutil.Eventually(re, func() bool {
		if syncErr == nil {
			select {
			case syncErr = <-done:
			default:
				return false
			}
		}
		return errors.Is(syncErr, errHistoryBufferRetainOverflow)
	})
	re.Empty(syncer.GetAllDownstreamNames())
}

func TestClientWaitsForFullSyncCompletionBeforeRunning(t *testing.T) {
	re := require.New(t)
	regionStorage := storage.NewStorageWithMemoryBackend()
	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		regionStorage,
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	bc := core.NewBasicCluster()
	fullSyncing := false
	region := &metapb.Region{
		Id:          1,
		StartKey:    []byte{1},
		EndKey:      []byte{2},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{{Id: 11, StoreId: 1}},
	}

	syncer.handleRegionSyncResponse(context.Background(), &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		Regions:    []*metapb.Region{region},
		StartIndex: 0,
	}, bc, regionStorage, &fullSyncing)
	re.True(fullSyncing)
	re.False(syncer.IsRunning())

	syncer.handleRegionSyncResponse(context.Background(), &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		StartIndex: 1,
	}, bc, regionStorage, &fullSyncing)
	re.False(fullSyncing)
	re.True(syncer.IsRunning())
}

func newTestRegionSyncer(t *testing.T, regions ...*core.RegionInfo) (*RegionSyncer, *core.BasicCluster) {
	t.Helper()
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	t.Cleanup(func() {
		re.NoError(regionStorage.Close())
	})

	bc := core.NewBasicCluster()
	for _, region := range regions {
		bc.PutRegion(region)
	}
	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), regionStorage),
		bc,
	)
	return NewRegionSyncer(server), bc
}

func newTestSyncRegion(regionID, peerID uint64) *core.RegionInfo {
	return core.NewRegionInfo(&metapb.Region{
		Id:          regionID,
		StartKey:    []byte{byte(regionID)},
		EndKey:      []byte{byte(regionID + 1)},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{{Id: peerID, StoreId: 1}},
	}, &metapb.Peer{Id: peerID, StoreId: 1})
}

func startTestRegionSync(ctx context.Context, syncer *RegionSyncer, stream *mockSyncRegionsServer) chan error {
	done := make(chan error, 1)
	go func() {
		done <- syncer.Sync(ctx, stream)
	}()
	return done
}

func sendTestSyncRegionRequest(stream *mockSyncRegionsServer, startIndex uint64) {
	stream.recvCh <- &pdpb.SyncRegionRequest{
		Header:     &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		StartIndex: startIndex,
		Member: &pdpb.Member{
			Name:       "pd-follower",
			ClientUrls: []string{"http://127.0.0.1:2379"},
		},
	}
}

func mustRecvSyncRegionResponse(t *testing.T, stream *mockSyncRegionsServer, message string) *pdpb.SyncRegionResponse {
	t.Helper()
	select {
	case resp := <-stream.sendCh:
		return resp
	case <-time.After(3 * time.Second):
		require.FailNow(t, message)
		return nil
	}
}

func waitTestRegionSyncerBound(re *require.Assertions, syncer *RegionSyncer) {
	testutil.Eventually(re, func() bool {
		names := syncer.GetAllDownstreamNames()
		return len(names) == 1 && names[0] == "pd-follower"
	})
}

func waitTestRegionSyncerUnavailable(re *require.Assertions, done <-chan error) {
	var syncErr error
	testutil.Eventually(re, func() bool {
		if syncErr == nil {
			select {
			case syncErr = <-done:
			default:
				return false
			}
		}
		st, ok := status.FromError(syncErr)
		return ok && st.Code() == codes.Unavailable
	})
}

type mockSyncRegionsServer struct {
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	recvCh  chan *pdpb.SyncRegionRequest
	sendCh  chan *pdpb.SyncRegionResponse
	sendErr error
	blockCh chan struct{}
	blocked chan struct{}
	once    sync.Once
}

func newMockSyncRegionsServer() *mockSyncRegionsServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &mockSyncRegionsServer{
		ctx:    ctx,
		cancel: cancel,
		recvCh: make(chan *pdpb.SyncRegionRequest),
		sendCh: make(chan *pdpb.SyncRegionResponse, 1),
	}
}

func (s *mockSyncRegionsServer) Send(resp *pdpb.SyncRegionResponse) error {
	s.mu.Lock()
	err := s.sendErr
	blockCh := s.blockCh
	blocked := s.blocked
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if blockCh != nil {
		if blocked != nil {
			s.once.Do(func() {
				close(blocked)
			})
		}
		<-blockCh
	}
	s.sendCh <- resp
	return nil
}

func (s *mockSyncRegionsServer) setSendErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendErr = err
}

func (s *mockSyncRegionsServer) blockSend() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockCh = make(chan struct{})
	s.blocked = make(chan struct{})
	s.once = sync.Once{}
	return s.blockCh
}

func (s *mockSyncRegionsServer) isSendBlocked() bool {
	s.mu.Lock()
	blocked := s.blocked
	s.mu.Unlock()
	if blocked == nil {
		return false
	}
	select {
	case <-blocked:
		return true
	default:
		return false
	}
}

func (s *mockSyncRegionsServer) Recv() (*pdpb.SyncRegionRequest, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case req, ok := <-s.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return req, nil
	}
}

func (*mockSyncRegionsServer) SetHeader(metadata.MD) error {
	return nil
}

func (*mockSyncRegionsServer) SendHeader(metadata.MD) error {
	return nil
}

func (*mockSyncRegionsServer) SetTrailer(metadata.MD) {}

func (s *mockSyncRegionsServer) Context() context.Context {
	return s.ctx
}

func (*mockSyncRegionsServer) SendMsg(any) error {
	return nil
}

func (*mockSyncRegionsServer) RecvMsg(any) error {
	return nil
}
