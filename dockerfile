FROM golang:1.26-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /top-queries

COPY app/go.mod app/go.sum /top-queries/
RUN go mod download

COPY app/ /top-queries/

RUN go build -ldflags="-s -w" -o build/main cmd/main.go

FROM alpine:3.20 AS runner

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /top-queries/build/main /app/main

CMD [ "/app/main" ]