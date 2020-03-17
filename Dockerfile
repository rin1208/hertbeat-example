FROM golang:latest as go-builder
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GO111MODULE=on
WORKDIR /app
COPY . /app
# RUN go build main.go 

# FROM ubuntu

# WORKDIR /app
# EXPOSE 8080
# COPY --from=go-builder /app/main  .
# # CMD /app/main