FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/bridge ./cmd/bridge && CGO_ENABLED=0 go build -o /out/demo-echo ./cmd/demo-echo

FROM alpine:3.22
RUN addgroup -S bridge && adduser -S bridge -G bridge
COPY --from=build /out/bridge /out/demo-echo /usr/local/bin/
USER bridge
ENTRYPOINT ["bridge"]
CMD ["-config", "/etc/bridge/config.yaml"]
