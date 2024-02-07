FROM golang:1.22-alpine as builder

ENV APP_HOME /go/src/github.com/shopmonkeyus/eds-server

WORKDIR "$APP_HOME"
COPY . .

RUN go mod download && go mod verify && go build -o eds-server

FROM alpine:3.17

ENV APP_HOME /go/src/github.com/shopmonkeyus/eds-server
RUN mkdir -p "$APP_HOME"
WORKDIR "$APP_HOME"

ARG GIT_SHA
ARG GIT_BRANCH
ARG BUILD_DATE

COPY --from=builder "$APP_HOME"/eds-server $APP_HOME

ENV GIT_SHA $GIT_SHA
ENV GIT_BRANCH $GIT_BRANCH
ENV BUILD_DATE $BUILD_DATE

EXPOSE 8080

CMD ["./eds-server"]