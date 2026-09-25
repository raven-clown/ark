FROM node:22-alpine AS web
WORKDIR /web
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod main.go ./
COPY --from=web /web/dist ./dist
RUN CGO_ENABLED=0 go build -o /out/console .

FROM alpine:3.22
RUN addgroup -S console && adduser -S console -G console
COPY --from=build /out/console /usr/local/bin/console
USER console
EXPOSE 8088
ENTRYPOINT ["console"]
