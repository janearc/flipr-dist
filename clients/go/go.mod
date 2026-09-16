// The Go client is its own module ON PURPOSE. A consumer importing it must
// not inherit flipr's server dependencies -- franz-go, bbolt, and
// big-little-mesh -- when all it wants is to ask a question.
//
// One dependency, and it is the one the wire is made of.
module github.com/janearc/flipr-dist/clients/go

go 1.26.4

require google.golang.org/protobuf v1.36.12
