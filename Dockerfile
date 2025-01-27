FROM golang:1.22-alpine AS build
WORKDIR /go/src/dls
COPY . .
RUN CGO_ENABLED=0 go build -v -o /go/bin/dls ./cmd/dls
RUN GRPC_HEALTH_PROBE_VERSION=v0.3.2 &&  wget -qO/go/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 &&  chmod +x /go/bin/grpc_health_probe


FROM scratch
COPY --from=build /go/bin/dls /bin/dls
COPY --from=build /go/bin/grpc_health_probe /bin/grpc_health_probe
ENTRYPOINT ["/bin/dls"]