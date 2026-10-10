module tsession/contrib/tredis

go 1.26.0

require tsession v0.0.0

require (
	github.com/alicebob/miniredis/v2 v2.40.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/redis/go-redis/v9 v9.23.0 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace tsession => ../..
