FROM --platform=$BUILDPLATFORM node:22-alpine AS web
WORKDIR /web
COPY bridge-ui/package.json bridge-ui/package-lock.json ./
RUN npm ci
COPY bridge-ui/ .
RUN npm run build

FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY bridge-engine/go.mod bridge-engine/go.sum ./
RUN go mod download
COPY bridge-engine/ .
COPY --from=web /web/dist ./internal/webui/dist
RUN export CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH && \
    go build -ldflags "-s -w -X main.version=$VERSION" -o /out/ark ./cmd/bridge && \
    go build -ldflags "-s -w" -o /out/demo-echo ./cmd/demo-echo

FROM alpine:3.22
RUN addgroup -S -g 10001 ark && adduser -S -u 10001 ark -G ark && mkdir /data && chown ark:ark /data
COPY --from=build /out/ark /out/demo-echo /usr/local/bin/
USER ark
ENV ARK_CONFIG_FILE=/data/config.yaml
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["ark"]
