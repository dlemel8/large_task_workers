module processor

go 1.16

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.9.0
	google.golang.org/grpc v1.40.0
	protos v0.0.0
)

replace protos => ../protos/
