// Package e2e contains end-to-end tests that start a real Criceta cluster.
//
// The tests are guarded by the "e2e" build tag so that the default
// `go test ./...` run stays fast and hermetic:
//
//	go test -tags=e2e -v ./test/e2e
package e2e
