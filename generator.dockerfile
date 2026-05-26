FROM golang:1.26-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /query-gen

COPY app/go.mod app/go.sum /query-gen/
RUN go mod download

COPY app/ /query-gen/

RUN go build -ldflags="-s -w" -o build/main generator/main.go

FROM alpine:3.20 AS runner

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /query-gen/build/main /app/main

ENTRYPOINT [ "/app/main" ]