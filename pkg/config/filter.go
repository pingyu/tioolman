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

package config

import (
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/parser/model"
)

// FilterConfig represents filter config for a changefeed
type FilterConfig struct {
	Rules []string `toml:"rules" json:"rules"`
	*filter.MySQLReplicationRules
	IgnoreTxnStartTs []uint64           `toml:"ignore-txn-start-ts" json:"ignore-txn-start-ts"`
	DDLAllowlist     []model.ActionType `toml:"ddl-allow-list" json:"ddl-allow-list,omitempty"`
}
