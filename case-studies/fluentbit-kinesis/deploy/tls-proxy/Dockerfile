# Fluent Bit's official `kinesis_streams` output plugin unconditionally
# wraps its upstream connection in TLS (it hardcodes FLB_IO_TLS when
# creating the upstream, in every released version through at least 3.2.x)
# -- there is no config option to speak plain HTTP to a custom endpoint,
# unlike some other AWS-for-Fluent-Bit outputs. aws2azure itself doesn't
# terminate TLS (by design: a sidecar proxy typically sits behind
# infrastructure-level TLS termination, or plain HTTP on a private
# network). This image bridges that gap for the case study only: a
# self-signed cert + nginx terminates TLS and forwards to aws2azure in
# plaintext, preserving the original Host header so aws2azure's
# Host-prefix routing ("kinesis.") still matches.
FROM alpine:3.19 AS cert
RUN apk add --no-cache openssl \
    && mkdir -p /tls \
    && openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
         -keyout /tls/key.pem -out /tls/cert.pem \
         -subj "/CN=kinesis.tls.aws2azure"

FROM nginx:1.27-alpine
COPY --from=cert /tls/cert.pem /etc/nginx/tls/cert.pem
COPY --from=cert /tls/key.pem /etc/nginx/tls/key.pem
COPY nginx.conf /etc/nginx/conf.d/default.conf
