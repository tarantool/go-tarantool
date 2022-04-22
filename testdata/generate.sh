#!/usr/bin/env bash
set -xeuo pipefail
# An example how-to re-generate testing certificates (because usually
# TLS certificates have expiration dates and some day they will expire).
#
# The instruction is valid for:
#
# $ openssl version
# OpenSSL 3.0.2 15 Mar 2022 (Library: OpenSSL 3.0.2 15 Mar 2022)

cat <<EOF > domains.ext
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

openssl req -x509 -nodes -new -sha256 -days 8192 -newkey rsa:2048 -keyout ca.key -out ca.pem -subj "/C=US/CN=Example-Root-CA"
openssl x509 -outform pem -in ca.pem -out ca.crt

openssl req -new -nodes -newkey rsa:2048 -keyout localhost.key -out localhost.csr -subj "/C=US/ST=YourState/L=YourCity/O=Example-Certificates/CN=localhost"
openssl x509 -req -sha256 -days 8192 -in localhost.csr -CA ca.pem -CAkey ca.key -CAcreateserial -extfile domains.ext -out localhost.crt
