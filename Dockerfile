# One image, three binaries. docker-compose picks which one to run, so the API
# and scheduler can never drift apart in dependencies or Go version.
FROM golang:1.25-alpine AS build

WORKDIR /src
# Copy manifests first so dependency downloads are cached across code changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/api       ./cmd/api      && \
    CGO_ENABLED=0 go build -trimpath -o /out/scheduler ./cmd/scheduler && \
    CGO_ENABLED=0 go build -trimpath -o /out/runner    ./cmd/runner

FROM alpine:3.20
# git is needed by the runner to check repositories out; ca-certificates by the
# GitHub client.
RUN apk add --no-cache ca-certificates git && adduser -D -u 10001 forgerun

COPY --from=build /out/api       /usr/local/bin/api
COPY --from=build /out/scheduler /usr/local/bin/scheduler
COPY --from=build /out/runner    /usr/local/bin/runner

# The control plane never needs root.
USER forgerun
EXPOSE 8080
CMD ["/usr/local/bin/api"]
