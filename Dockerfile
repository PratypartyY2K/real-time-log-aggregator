# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build

ARG SERVICE
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY db ./db

RUN test "$SERVICE" = "ingest-api" -o "$SERVICE" = "processor" -o "$SERVICE" = "query-api"
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/logagg "./cmd/${SERVICE}"

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 10001 logagg \
    && adduser -S -D -H -u 10001 -G logagg logagg

COPY --from=build --chown=logagg:logagg /out/logagg /usr/local/bin/logagg

USER logagg
EXPOSE 8080 8081 9091
ENTRYPOINT ["/usr/local/bin/logagg"]
