FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/bridge ./cmd/bridge

FROM alpine:3.22
RUN addgroup -S bridge && adduser -S bridge -G bridge
COPY --from=build /out/bridge /usr/local/bin/bridge
USER bridge
ENTRYPOINT ["bridge"]
CMD ["-config", "/etc/bridge/config.yaml"]
