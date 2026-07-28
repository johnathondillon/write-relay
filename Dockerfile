FROM golang:1.26.5-alpine3.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/writerelayd ./cmd/writerelayd

FROM alpine:3.23
RUN apk add --no-cache ca-certificates && \
    addgroup -S writerelay && adduser -S -G writerelay writerelay && \
    mkdir -p /var/lib/writerelay && \
    chown writerelay:writerelay /var/lib/writerelay
WORKDIR /var/lib/writerelay
COPY --from=build /out/writerelayd /usr/local/bin/writerelayd
USER writerelay
ENTRYPOINT ["writerelayd"]
CMD ["version"]
