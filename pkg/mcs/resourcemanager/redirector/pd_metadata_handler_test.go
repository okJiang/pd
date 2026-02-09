// Copyright 2025 TiKV Project Authors.
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

package redirector

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/metering"
	"github.com/tikv/pd/pkg/storage"
)

type mockPDManagerProvider struct {
	bs.Server
}

func (*mockPDManagerProvider) GetControllerConfig() *server.ControllerConfig {
	return &server.ControllerConfig{}
}

func (*mockPDManagerProvider) GetMeteringWriter() *metering.Writer { return nil }

func (*mockPDManagerProvider) GetResourceGroupWriteRole() server.ResourceGroupWriteRole {
	return server.ResourceGroupWriteRolePDMetaOnly
}

func (*mockPDManagerProvider) AddStartCallback(...func()) {}

func (*mockPDManagerProvider) AddServiceReadyCallback(...func(context.Context) error) {}

func TestShouldHandlePDMetadataLocally(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	tests := []struct {
		method string
		path   string
		expect bool
	}{
		{http.MethodPost, "/resource-manager/api/v1/config/group", true},
		{http.MethodPut, "/resource-manager/api/v1/config/group", true},
		{http.MethodGet, "/resource-manager/api/v1/config/groups", true},
		{http.MethodGet, "/resource-manager/api/v1/config/group/test", true},
		{http.MethodDelete, "/resource-manager/api/v1/config/group/test", true},
		{http.MethodPost, "/resource-manager/api/v1/config/keyspace/service-limit", true},
		{http.MethodGet, "/resource-manager/api/v1/config/keyspace/service-limit/test", true},
		{http.MethodGet, "/resource-manager/api/v1/config", false},
		{http.MethodPut, "/resource-manager/api/v1/admin/log", false},
	}
	for _, tc := range tests {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		re.Equal(tc.expect, shouldHandlePDMetadataLocally(req), "method=%s path=%s", tc.method, tc.path)
	}
}

func TestPDMetadataHandler(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	manager := server.NewManager[*mockPDManagerProvider](&mockPDManagerProvider{})
	manager.SetStorageForTest(storage.NewStorageWithMemoryBackend())
	handler := newPDMetadataHandler(manager)

	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}

	resp := doJSONRequest(re, handler, http.MethodPost, "/resource-manager/api/v1/config/group", group)
	re.Equal(http.StatusOK, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodGet, "/resource-manager/api/v1/config/group/test_group", nil)
	re.Equal(http.StatusOK, resp.Code)

	group.Priority = 9
	resp = doJSONRequest(re, handler, http.MethodPut, "/resource-manager/api/v1/config/group", group)
	re.Equal(http.StatusOK, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodPost, "/resource-manager/api/v1/config/keyspace/service-limit", map[string]float64{
		"service_limit": 12.5,
	})
	re.Equal(http.StatusOK, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodGet, "/resource-manager/api/v1/config/keyspace/service-limit", nil)
	re.Equal(http.StatusOK, resp.Code)
	var limiter struct {
		ServiceLimit float64 `json:"service_limit"`
	}
	re.NoError(json.Unmarshal(resp.Body.Bytes(), &limiter))
	re.Equal(12.5, limiter.ServiceLimit)

	resp = doJSONRequest(re, handler, http.MethodDelete, "/resource-manager/api/v1/config/group/test_group", nil)
	re.Equal(http.StatusOK, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodGet, "/resource-manager/api/v1/config/group/test_group", nil)
	re.Equal(http.StatusNotFound, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodPost, "/resource-manager/api/v1/config/controller", map[string]any{
		"unknown": 1,
	})
	re.Equal(http.StatusBadRequest, resp.Code)

	resp = doJSONRequest(re, handler, http.MethodGet, "/resource-manager/api/v1/config/keyspace/service-limit/non-existing", nil)
	re.Equal(http.StatusBadRequest, resp.Code)
}

func doJSONRequest(re *require.Assertions, handler http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	var reqBody *bytes.Buffer
	if body != nil {
		data, err := json.Marshal(body)
		re.NoError(err)
		reqBody = bytes.NewBuffer(data)
	} else {
		reqBody = bytes.NewBuffer(nil)
	}
	req := httptest.NewRequest(method, path, reqBody)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	return resp
}
