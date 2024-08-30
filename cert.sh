#!/bin/bash

mkdir -p certs

VALIDITY_DAYS=365 # 1 years

# Create CA (certificate authority) key
openssl genrsa -out certs/ca.key 2048

# Create a root CA certificate which is valid for ${VALIDITY_DAYS}
openssl req -new -x509 -days ${VALIDITY_DAYS} -key certs/ca.key -subj "/CN=Acme Root CA" -out certs/ca.crt

#################################################Server#################################################
# Create server certificate signing request
openssl req -newkey rsa:2048 -nodes -keyout certs/server.key -subj "/CN=localhost" -out certs/server.csr

# Create server certificate
openssl x509 -req -extfile <(printf "subjectAltName=DNS:localhost") -days ${VALIDITY_DAYS} -in certs/server.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/server.crt

#################################################Client#################################################
# Create certificate signing request for client
openssl req -newkey rsa:2048 -nodes -keyout certs/client.key -subj "/CN=localhost" -out certs/client.csr

# Create client certificate
openssl x509 -req -extfile <(printf "subjectAltName=DNS:localhost") -days ${VALIDITY_DAYS} -in certs/client.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/client.crt

# Generate private key for signing JWT tokens
openssl genrsa -out certs/jwt_private.pem 4096 

# Extract a public key out of generate private key
openssl rsa -in certs/jwt_private.pem -pubout -out certs/jwt_public.pem 

# Remove signing request certificates
rm certs/server.csr
rm certs/client.csr
rm certs/ca.srl