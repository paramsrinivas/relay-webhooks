FROM golang:1.23 AS build

WORKDIR /src

COPY . .

RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux go build -o /relay ./cmd/relay

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /relay /relay

EXPOSE 8080

ENTRYPOINT ["/relay"]
