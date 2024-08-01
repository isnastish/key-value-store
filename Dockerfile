# build environment
FROM golang:1.22 AS build-env

WORKDIR /go/src/github.com/isnastish/kvs/services/kvs

ADD . /go/src/github.com/isnastish/kvs/services/kvs

RUN CGO_ENABLED=0 GOOS=linux go build -a -v -o /go/bin/services/kvs \
    github.com/isnastish/kvs/services/kvs

# production environment
FROM golang:1.22-alpine AS run-env

COPY --from=build-env /go/bin/services/kvs /kvs/

# Expose port 8080 for TCP connection (to be used with -P flag)
# Exposed port can be ovewritten with --publish option used with docker run
EXPOSE 8080/tcp

ENTRYPOINT [ "/kvs/kvs" ]