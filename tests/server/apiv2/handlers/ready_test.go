// Copyright 2024 TiKV Project Authors.
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

package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

func TestReadyAPI(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	re.NoError(leader.BootstrapCluster())
	leaderURL := leader.GetConfig().ClientUrls + v2Prefix + "/ready"
	followerServer := cluster.GetServer(cluster.GetFollower())
	followerURL := followerServer.GetConfig().ClientUrls + v2Prefix + "/ready"
	checkReadyAPI(re, leaderURL, true)
	checkReadyAPI(re, followerURL, true)

	// check ready status when region is not loaded for leader
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/loadRegionSlow", `return("`+leader.GetAddr()+`")`))
	checkReadyAPI(re, leaderURL, false)
	checkReadyAPI(re, followerURL, true)

	// check ready status when region is not loaded for follower
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/loadRegionSlow", `return("`+followerServer.GetAddr()+`")`))
	checkReadyAPI(re, leaderURL, true)
	checkReadyAPI(re, followerURL, false)

	re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/loadRegionSlow"))
}

func checkReadyAPI(re *require.Assertions, url string, isReady bool) {
	expectCode := http.StatusOK
	if !isReady {
		expectCode = http.StatusInternalServerError
	}
	// check ready status
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	re.NoError(err)
	re.Empty(buf)
	re.Equal(expectCode, resp.StatusCode)
	// check ready status with verbose
	req, err = http.NewRequest(http.MethodGet, url+"?verbose", http.NoBody)
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	buf, err = io.ReadAll(resp.Body)
	re.NoError(err)
	r := &handlers.ReadyStatus{}
	re.NoError(json.Unmarshal(buf, &r))
	re.Equal(expectCode, resp.StatusCode)
	re.Equal(isReady, r.RegionLoaded)
}
