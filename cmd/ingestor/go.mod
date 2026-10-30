module github.com/corescope/ingestor

go 1.22

require (
	github.com/eclipse/paho.mqtt.golang v1.5.0
	github.com/meshcore-analyzer/geofilter v0.0.0
	github.com/meshcore-analyzer/sigvalidate v0.0.0
)

replace github.com/meshcore-analyzer/geofilter => ../../internal/geofilter

replace github.com/meshcore-analyzer/sigvalidate => ../../internal/sigvalidate

require github.com/meshcore-analyzer/packetpath v0.0.0

replace github.com/meshcore-analyzer/packetpath => ../../internal/packetpath

require github.com/meshcore-analyzer/dbconfig v0.0.0

replace github.com/meshcore-analyzer/dbconfig => ../../internal/dbconfig

require github.com/meshcore-analyzer/perfio v0.0.0

replace github.com/meshcore-analyzer/perfio => ../../internal/perfio

require github.com/meshcore-analyzer/dbschema v0.0.0

replace github.com/meshcore-analyzer/dbschema => ../../internal/dbschema

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	golang.org/x/net v0.27.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
)

require github.com/meshcore-analyzer/prunequeue v0.0.0

replace github.com/meshcore-analyzer/prunequeue => ../../internal/prunequeue

require (
	github.com/mattn/go-sqlite3 v1.14.52
	github.com/meshcore-analyzer/mbcapqueue v0.0.0
)

replace github.com/meshcore-analyzer/mbcapqueue => ../../internal/mbcapqueue

require (
	github.com/lib/pq v1.10.9
	github.com/meshcore-analyzer/channel v0.0.0
	github.com/redis/go-redis/v9 v9.7.3
)

replace github.com/meshcore-analyzer/channel => ../../internal/channel
