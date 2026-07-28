// Package postgres embeds the PostgreSQL installation SQL for the setup command.
package postgres

import _ "embed"

//go:embed 001_install.sql
var InstallSQL string
