// Copyright 2018 TiKV Project Authors.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
)

func TestNormalizeHistoryBufferCapacity(t *testing.T) {
	testCases := []struct {
		name     string
		size     int
		unit     int
		expected int
	}{
		{name: "below-unit", size: 1, unit: historyBufferCapacityUnit, expected: historyBufferCapacityUnit},
		{name: "one-unit", size: historyBufferCapacityUnit, unit: historyBufferCapacityUnit, expected: historyBufferCapacityUnit},
		{name: "round-to-two-units", size: historyBufferCapacityUnit + 1, unit: historyBufferCapacityUnit, expected: 2 * historyBufferCapacityUnit},
		{name: "round-to-four-units", size: 3 * historyBufferCapacityUnit, unit: historyBufferCapacityUnit, expected: 4 * historyBufferCapacityUnit},
		{name: "test-unit", size: 3, unit: 1, expected: 4},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, normalizeHistoryBufferCapacity(testCase.size, testCase.unit))
		})
	}
}

func TestHistoryBufferKeepsRingBehaviorWithoutRetain(t *testing.T) {
	re := require.New(t)
	h := newTestHistoryBuffer(8)
	h.resetWithIndex(100)

	for i := 1; i <= 3; i++ {
		h.record(newHistoryBufferTestRegion(uint64(i)))
	}

	re.Equal(2, h.capacity())
	re.Nil(h.recordsFrom(100))
	records := h.recordsFrom(101)
	re.Len(records, 2)
	re.Equal(uint64(2), records[0].GetID())
	re.Equal(uint64(3), records[1].GetID())
}

func TestHistoryBufferPersistsNextIndexOnly(t *testing.T) {
	re := require.New(t)
	kvMem := kv.NewMemoryKV()
	h1 := newHistoryBufferWithConfig(4, 8, 1, kvMem)
	for i := 1; i <= 3; i++ {
		h1.record(newHistoryBufferTestRegion(uint64(i)))
	}
	re.Equal(3, h1.len())
	h1.persist()

	h2 := newHistoryBufferWithConfig(4, 8, 1, kvMem)
	re.Equal(uint64(3), h2.nextIndex())
	re.Equal(uint64(3), h2.firstIndex())
	re.Equal(0, h2.len())
	re.Nil(h2.get(2))
	s, err := h2.kv.Load(historyKey)
	re.NoError(err)
	re.Equal("3", s)
}

func TestHistoryBufferRetainGrowsAndPreservesRecords(t *testing.T) {
	re := require.New(t)
	h := newTestHistoryBuffer(8)
	h.resetWithIndex(100)

	retainer := h.retainFrom(100)
	defer retainer.release()
	for i := 1; i <= 3; i++ {
		h.record(newHistoryBufferTestRegion(uint64(i)))
	}

	re.Equal(8, h.capacity())
	re.False(retainer.overflowed())
	records := h.recordsFrom(100)
	re.Len(records, 3)
	for i, record := range records {
		re.Equal(uint64(i+1), record.GetID())
	}
}

func TestHistoryBufferMultipleRetainsKeepEarliestIndex(t *testing.T) {
	re := require.New(t)
	h := newTestHistoryBuffer(8)
	h.resetWithIndex(100)

	first := h.retainFrom(100)
	defer first.release()
	second := h.retainFrom(101)
	defer second.release()
	for i := 1; i <= 3; i++ {
		h.record(newHistoryBufferTestRegion(uint64(i)))
	}

	re.False(first.overflowed())
	re.False(second.overflowed())
	records := h.recordsFrom(100)
	re.Len(records, 3)
}

func TestHistoryBufferRetainOverflowAtMaxCapacity(t *testing.T) {
	re := require.New(t)
	h := newTestHistoryBuffer(4)
	h.resetWithIndex(100)

	retainer := h.retainFrom(100)
	defer retainer.release()
	for i := 1; i <= 5; i++ {
		h.record(newHistoryBufferTestRegion(uint64(i)))
	}

	re.Equal(4, h.capacity())
	re.True(retainer.overflowed())
}

func TestHistoryBufferObserveRequiredWindowGrowsWithoutRetain(t *testing.T) {
	re := require.New(t)
	h := newTestHistoryBuffer(8)

	h.observeRequiredWindow(3)

	re.Equal(8, h.capacity())
}

func TestHistoryBufferShrinksAfterRequiredWindowStaysLow(t *testing.T) {
	re := require.New(t)
	h := newTestHistoryBuffer(8)
	h.observeRequiredWindow(3)
	re.Equal(8, h.capacity())

	for range historyBufferShrinkRounds + 1 {
		h.observeRequiredWindow(1)
		h.maybeShrink()
	}

	re.Equal(2, h.capacity())
}

func newTestHistoryBuffer(maxCapacity int) *historyBuffer {
	return newHistoryBufferWithConfig(2, maxCapacity, 1, storage.NewStorageWithMemoryBackend())
}

func newHistoryBufferTestRegion(regionID uint64) *core.RegionInfo {
	return newTestSyncRegion(regionID, regionID+10)
}
