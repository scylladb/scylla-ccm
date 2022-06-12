FROM alpine

RUN apk --no-cache add sniproxy

EXPOSE 80 443
ENTRYPOINT ["sniproxy", "-f"]

