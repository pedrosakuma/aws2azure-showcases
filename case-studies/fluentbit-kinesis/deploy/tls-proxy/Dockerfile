# Fluent Bit's official `kinesis_streams` output plugin unconditionally
# wraps its upstream connection in TLS (it hardcodes FLB_IO_TLS when
# creating the upstream, in every released version through at least 3.2.x)
# -- there is no config option to speak plain HTTP to a custom endpoint,
# unlike some other AWS-for-Fluent-Bit outputs. It *also* hardcodes cert
# verification on for that connection (passes verify=TRUE to
# flb_tls_create() unconditionally), ignoring the generic `tls.verify off`
# output property that would normally disable it -- so a self-signed cert
# can only be trusted via `tls.ca_file` (which the plugin does forward).
#
# aws2azure itself doesn't terminate TLS (by design: a sidecar proxy
# typically sits behind infrastructure-level TLS termination, or plain HTTP
# on a private network). This image bridges that gap for the case study
# only: a fixed, checked-in self-signed cert/key pair (not a secret --
# purely a throwaway dev cert scoped to this ephemeral local/CI stack, the
# same convention as the other case studies' well-known emulator dev keys)
# terminates TLS via nginx and forwards to aws2azure in plaintext,
# preserving the original Host header so aws2azure's Host-prefix routing
# ("kinesis.") still matches. Fluent Bit trusts this same cert via
# `tls.ca_file` (see fluent-bit.conf).
FROM nginx:1.27-alpine
COPY cert.pem /etc/nginx/tls/cert.pem
COPY key.pem /etc/nginx/tls/key.pem
COPY nginx.conf /etc/nginx/conf.d/default.conf
