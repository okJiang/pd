// Copyright 2024 TiKV Authors
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

package realcluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/utils/tsoutil"
)

type clientSuite struct {
	clusterSuite
}

func TestClient(t *testing.T) {
	suite.Run(t, &clientSuite{
		clusterSuite: clusterSuite{
			suiteName: "client",
		},
	})
}

func (s *clientSuite) TestEnableForwarding() {
	re := require.New(s.T())
	ctx := context.Background()
	pdEndpoints := getPDEndpoints(s.T())
	client, err := pd.NewClientWithContext(
		ctx, caller.TestComponent, pdEndpoints,
		pd.SecurityOption{}, opt.WithForwardingOption(true),
	)
	re.NoError(err)

	var (
		lastTS uint64
		wg     sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			require.Eventually(s.T(), func() bool {
				physical, logical, err := client.GetMinTS(ctx)
				if err == nil {
					ts := tsoutil.ComposeTS(physical, logical)
					re.Less(lastTS, ts)
					lastTS = ts
				} else {
					s.T().Log(err)
				}
				return err == nil
			}, 3*time.Second, 100*time.Millisecond)
		}
	}()

	// leaderURL := client.GetLeaderURL()

	wg.Wait()
}
