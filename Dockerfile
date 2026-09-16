# flipr -- the flag store, containerised.
#
# THE IMAGE IS THE BINARY AND NOTHING ELSE. flipr's one runtime artifact is a
# static Go executable with the contract descriptor embedded in it; the bbolt
# file is STATE and state never ships in an image -- it lives on a volume the
# manifest mounts, or a restart would silently reset every flag to its
# published default, which is precisely the failure PublishNamespace was
# designed to prevent.
#
# Distroless static, not scratch: same empty attack surface, but with a
# non-root user already defined and CA certs present so a future kafka sink
# over TLS does not need a base-image change.
#
# No HEALTHCHECK: there is no shell in the final image to run one, and in the
# cluster the kubernetes probes own that question -- they hit /health, which
# reads the store rather than trusting that it opened.

FROM golang:1.26 AS build
WORKDIR /src

# module graph first, so a code change does not re-download dependencies
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# The commit hash arrives as a build arg and becomes the binary's Version --
# the same fact as the image tag and the flipr namespace key, container test
# clause 5: the service reports its version and the pod carries the same one.
ARG COMMIT=dev
# CGO off: bbolt is pure Go (chosen over RocksDB for exactly this) and a
# static binary is what lets the runtime image be empty.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.Version=$COMMIT" -o /flipr .

# Run the tests IN THE BUILD, so an image cannot exist whose code does not
# pass its own suite. The build is the gate; a broken flipr image is refused
# here rather than discovered in the cluster. With the race detector, because
# a race in the service would otherwise pass this gate; the
# detector needs cgo, so the test step runs with it on and the binary above
# is built without it.
RUN go vet ./... && CGO_ENABLED=1 go test -race ./...

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /flipr /flipr

# The pod overrides FLIPR_ADDR to bind beyond loopback; the default stays
# loopback so running the image ad hoc exposes nothing by accident.
ENV FLIPR_ADDR=127.0.0.1:15100 \
    FLIPR_DB=/state/flipr.db

EXPOSE 15100
ENTRYPOINT ["/flipr"]
