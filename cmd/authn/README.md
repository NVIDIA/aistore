Authentication Server (AuthN) provides a token-based secure access to AIStore.
It employs [JSON Web Tokens](https://github.com/golang-jwt/jwt) framework to grant access to resources:
buckets and objects. Please read a short [introduction to JWT](https://jwt.io/introduction/) for details.
Currently, we only support hash-based message authentication (HMAC) using SHA256 hash.

Further details at [AuthN documentation](/docs/authn.md).
