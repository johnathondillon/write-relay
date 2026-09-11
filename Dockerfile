FROM golang:1.26.8-alpine3.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${BUILD_DATE}" -o /out/writerelayd ./cmd/writerelayd

FROM alpine:3.23
ARG VERSION=dev
ARG COMMIT=unknown
LABEL org.opencontainers.image.title="WriteRelay" \
      org.opencontainers.image.source="https://github.com/johnathondillon/write-relay" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"
RUN apk add --no-cache ca-certificates && \
    addgroup -S writerelay && adduser -S -G writerelay writerelay && \
    mkdir -p /var/lib/writerelay && \
    chown writerelay:writerelay /var/lib/writerelay
WORKDIR /var/lib/writerelay
COPY --from=build /out/writerelayd /usr/local/bin/writerelayd
USER writerelay
ENTRYPOINT ["writerelayd"]
CMD ["version"]
