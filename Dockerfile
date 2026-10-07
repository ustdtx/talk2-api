# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build

WORKDIR /src

RUN apk add --no-cache ca-certificates

# Keep dependency downloads cacheable when application files change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/talk2-api ./cmd/api

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S talk2 \
    && adduser -S -G talk2 -H -D talk2

COPY --from=build /out/talk2-api /usr/local/bin/talk2-api

USER talk2
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:${PORT:-8080}/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/talk2-api"]
