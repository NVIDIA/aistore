## 1. rclone config

```console
$ cat $HOME/.config/rclone/rclone.conf
[ais]
type = s3
provider = Other
region = other-v2-signature
endpoint = http://localhost:8080/s3
acl = private
env_auth = false
```

## 2. aistore bucket

```console
$ ais create ais://nnn
```

## 3. copy local directory

```console
$ rclone copy /tmp/www ais://nnn
```
