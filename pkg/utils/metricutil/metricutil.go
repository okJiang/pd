// Copyright 2016 TiKV Project Authors.
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

package metricutil

import (
	"context"
	"os"
	"runtime"
	"time"
	"unicode"

	"github.com/grafana/pyroscope-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const zeroDuration = time.Duration(0)

// MetricConfig is the metric configuration.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetricConfig struct {
	PushJob      string            `toml:"job" json:"job"`
	PushAddress  string            `toml:"address" json:"address"`
	PushInterval typeutil.Duration `toml:"interval" json:"interval"`
}

func runesHasLowerNeighborAt(runes []rune, idx int) bool {
	if idx > 0 && unicode.IsLower(runes[idx-1]) {
		return true
	}
	if idx+1 < len(runes) && unicode.IsLower(runes[idx+1]) {
		return true
	}
	return false
}

func camelCaseToSnakeCase(str string) string {
	runes := []rune(str)
	length := len(runes)

	var ret []rune
	for i := range length {
		if i > 0 && unicode.IsUpper(runes[i]) && runesHasLowerNeighborAt(runes, i) {
			ret = append(ret, '_')
		}
		ret = append(ret, unicode.ToLower(runes[i]))
	}

	return string(ret)
}

// prometheusPushClient pushes metrics to Prometheus Pushgateway.
func prometheusPushClient(ctx context.Context, job, addr string, interval time.Duration) {
	defer logutil.LogPanic()

	pusher := push.New(addr, job).
		Gatherer(prometheus.DefaultGatherer).
		Grouping("instance", instanceName())

	for {
		err := pusher.Push()
		if err != nil {
			log.Error("could not push metrics to Prometheus Pushgateway", errs.ZapError(errs.ErrPrometheusPushMetrics, err))
		}

		select {
		case <-ctx.Done():
			log.Info("stop Prometheus push client")
			return
		case <-time.After(interval):
			// continue to push metrics
		}
	}
}

// Push metrics in background.
func Push(ctx context.Context, cfg *MetricConfig) {
	if cfg.PushInterval.Duration == zeroDuration || len(cfg.PushAddress) == 0 {
		log.Info("disable Prometheus push client")
		return
	}

	log.Info("start Prometheus push client")

	interval := cfg.PushInterval.Duration
	go prometheusPushClient(ctx, cfg.PushJob, cfg.PushAddress, interval)
}

func instanceName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

// EnablePyroscope enables pyroscope if pyroscope is enabled.
func EnablePyroscope() {
	if os.Getenv("PYROSCOPE_SERVER_ADDRESS") != "" {
		runtime.SetMutexProfileFraction(5)
		runtime.SetBlockProfileRate(5)
		_, err := pyroscope.Start(pyroscope.Config{
			ApplicationName:   "pd",
			ServerAddress:     os.Getenv("PYROSCOPE_SERVER_ADDRESS"),
			Logger:            pyroscope.StandardLogger,
			AuthToken:         os.Getenv("PYROSCOPE_AUTH_TOKEN"),
			TenantID:          os.Getenv("PYROSCOPE_TENANT_ID"),
			BasicAuthUser:     os.Getenv("PYROSCOPE_BASIC_AUTH_USER"),
			BasicAuthPassword: os.Getenv("PYROSCOPE_BASIC_AUTH_PASSWORD"),
			ProfileTypes: []pyroscope.ProfileType{
				pyroscope.ProfileCPU,
				pyroscope.ProfileAllocSpace,
			},
			UploadRate: 30 * time.Second,
		})
		if err != nil {
			log.Fatal("fail to start pyroscope", zap.Error(err))
		}
	}
}
