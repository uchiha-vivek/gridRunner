// Package migrations embeds the SQL schema files so the binaries carry their own
// migrations and no separate CLI tool is needed to boot the system.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
