// Copyright 2023 TiKV Authors
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
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/stretchr/testify/require"
)

const physicalShiftBits = 18

// GetTimeFromTS extracts time.Time from a timestamp.
func GetTimeFromTS(ts uint64) time.Time {
	ms := ExtractPhysical(ts)
	return time.Unix(ms/1e3, (ms%1e3)*1e6)
}

// ExtractPhysical returns a ts's physical part.
func ExtractPhysical(ts uint64) int64 {
	return int64(ts >> physicalShiftBits)
}

func runCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runCommandWithOutput(cmdStr string) ([]string, error) {
	cmd := exec.Command("sh", "-c", cmdStr)
	bytes, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	output := strings.Split(string(bytes), "\n")
	return output, nil
}

func killProcess(t require.TestingT, grepStr string) {
	cmdStr := fmt.Sprintf("ps -ef | grep %s | awk '{print $2}'", grepStr)
	cmd := exec.Command("sh", "-c", cmdStr)
	bytes, err := cmd.Output()
	require.NoError(t, err)
	pids := string(bytes)
	pidArr := strings.Split(pids, "\n")
	for _, pid := range pidArr {
		// nolint:errcheck
		runCommand("sh", "-c", "kill -9 "+pid)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
