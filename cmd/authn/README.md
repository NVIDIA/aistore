---
layout: post
title: AUTHN
permalink: cmd/authn
redirect_from:
 - cmd/authn/README.md/
---

AIStore Authentication Server (AuthN) provides a token-based secure access to AIStore.
It employs [JSON Web Tokens](https://github.com/dgrijalva/jwt-go) framework to grant access to resources:
buckets and objects. Please read a short [introduction to JWT](https://jwt.io/introduction/) for details.
Currently, we only support hash-based message authentication (HMAC) using SHA256 hash.

For more details, see [AuthN documentation](/aistore/docs/authn.md).
