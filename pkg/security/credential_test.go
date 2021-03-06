// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package security

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetCommonName(t *testing.T) {
	cd := &Credential{
		CAPath:   "../../tests/_certificates/ca.pem",
		CertPath: "../../tests/_certificates/server.pem",
		KeyPath:  "../../tests/_certificates/server-key.pem",
	}
	cn, err := cd.getSelfCommonName()
	require.Nil(t, err)
	require.Equal(t, "tidb-server", cn)

	cd.CertPath = "../../tests/_certificates/server-key.pem"
	_, err = cd.getSelfCommonName()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to decode PEM block to certificate")
}
