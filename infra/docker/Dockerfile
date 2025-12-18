FROM golang:1.25-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN go build -o /api-server ./cmd/api-server

CMD [ "/api-server" ]
