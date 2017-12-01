FROM golang:1.9.2

RUN apt-get update && \
    apt-get install -y libglib2.0-dev liblvm2-dev

RUN mkdir -p /go/src/github.com/alecthomas && \
    cd /go/src/github.com/alecthomas && \
    git clone https://github.com/alecthomas/gometalinter.git --branch=v1.2.1 && \
    go install -v github.com/alecthomas/gometalinter && \
    gometalinter --install && \
    mkdir -p /go/src/github.com/mesosphere
