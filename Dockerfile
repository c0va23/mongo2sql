FROM golang:1.7-alpine

ADD . /go/src/github.com/c0va23/mongo2sql

# TODO: Not use `go get`
RUN go install github.com/c0va23/mongo2sql && \
    mv /go/bin/mongo2sql /usr/bin/ && \
    rm -r /go && \
    mkdir /data

WORKDIR /data

CMD mongo2sql
