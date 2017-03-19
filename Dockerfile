FROM golang:1.7.4

ADD . /go/src/github.com/c0va23/mongo2sql

# TODO: Not use `go get`
RUN go get github.com/c0va23/mongo2sql && \
    go install github.com/c0va23/mongo2sql && \
    rm -r /go/src && \
    mkdir /data

WORKDIR /data

CMD mongo2sql
