FROM golang:alpine

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux

WORKDIR /opt/project

RUN go mod tidy && go mod tidy && go build -v ./...