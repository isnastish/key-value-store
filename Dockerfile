# build environment
FROM golang:1.22 as build-env

WORKDIR /go/src/github.com/isnastish/kvs/services/kvs

ADD . /go/src/github.com/isnastish/kvs/services/kvs

RUN CGO_ENABLED=0 GOOS=linux go build -a -v -o /go/bin/services/kvs \
    github.com/isnastish/kvs/services/kvs

# production environment
FROM golang:1.22-alpine as run-env

COPY --from=build-env /go/bin/services/kvs /kvs/

ENTRYPOINT [ "/kvs/kvs" ]

CMD [ "--endpoint",  ":8080"]