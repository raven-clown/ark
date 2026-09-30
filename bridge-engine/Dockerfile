FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN export CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH && \
    go build -ldflags "-s -w -X main.version=$VERSION" -o /out/bridge ./cmd/bridge && \
    go build -ldflags "-s -w" -o /out/demo-echo ./cmd/demo-echo

FROM alpine:3.24
RUN addgroup -S -g 10001 bridge && adduser -S -u 10001 bridge -G bridge
COPY --from=build /out/bridge /out/demo-echo /usr/local/bin/
USER bridge
ENTRYPOINT ["bridge"]
CMD ["-config", "/etc/bridge/config.yaml"]
