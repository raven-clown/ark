FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/bridge ./cmd/bridge

FROM alpine:3.20
COPY --from=build /out/bridge /usr/local/bin/bridge
ENTRYPOINT ["bridge"]
CMD ["-config", "/etc/bridge/config.yaml"]
