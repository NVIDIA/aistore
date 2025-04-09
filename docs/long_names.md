AIStore fully supports extremely long object names through both its native API and S3-compatible API. This flexibility allows for complex naming schemes, deeply nested directory structures, and detailed metadata encoding within object names.

# Table of Contents
- [Example: Using S3-Compatible API](#example-using-s3-compatible-api)
- [Example: Using Native API](#example-using-native-api)
- [Features](#features)
- [References](#references)

## Example: Using S3-Compatible API

```console
#
# 1. Generate a 1024-character random string as our object name
#
$ export longname=$(tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 1024)

#
# 2. Upload a file with the extremely long name via S3 API
#
$ s3cmd put README.md s3://mybucket/$longname

upload: 'README.md' -> 's3://nnn/vB4yvlo5gh6XWXDa6vR9K6klxqxP7upf94ert8ncT5a8GJ5BPCQSvK0Jc1Jxyp9U8Ek643yFJ9MFFAdwv8TP7UynvGWvUI94nRwkpil9zpgNFxbEja237kklSwiVc9DulCLDXF25YwBFWuHINNVvU27fMy56l4vgoxyNhdKYRg4L45Bq5gyluRFSgMIuhOk3xcQqAIG62g4DB0Tex5q8Ap0JOOHIlm3jNBYVf2QcFVVBOZefsQeggyMjya1NVsTmDaYLRK0oUR970FV8BAWHo05yQW4S11FkPmAy8WbsaQqKtmO2qjJlnnflZ3ItE1Nt13Tb4ZPdVzm5Ev5DQiUGgHb9kgLXa43gsIy1sc1mWbB5PkDVoKpOHCXNaHsjKXBZ0JlvoGYh7PrJ5GhMcsxjbZqccoBsSe0u7tPesn4Zfvvx754VRmBD1W8MzQel2YmoDRLj3k2992odyWxp0zke68PpulRuLZIGQGSecSOivfyouqMz2YQ7lWl5hod0qGVN4VZTtCgSaScntOmpy1aAYDxNDm5uI601JtMIHMpXMIxyndYPAel0PH4HzFQ4dBiQDERWogoHRN4eU1vflZFpOesdyxIYhcR6VgZI9JFDdKlAZwzSv136UJ876lD4I1DEqGL03dLmEsitgn2uNE515Fa3srEbeb5KMWDQ2dfTUPr6NJEqvPZrObFJdRgrk2eWeajFWjl7TtxwgyjuqMye320R3z11SAaVoOa66As4ZHGz7YsDo3lSfdyFA2tFdvLIVvLEzpKWhoZyV8NATTuWYfDKotJU7LlvfDfXqsq25A6ycMYcGQn6i3n74Xo1Xpd8Ah88jWF8KCVviLr929Qy5UJTAQAwPUMFedW5ckrWPPX4Lc1Z7j7F0Zhf6vvKdgTX7WtzqFT4OmrLtbMXiSVSAUL3NELa9sc4DRScZvfpT11UMnAz0n8X8vjnaFUDPmwRuEcaO99tPN8g7fpLiMJYQXv1VCRzJlkrmipwCqc275mcvUOkBEx189DJ3Sq0zwIi'  [1 of 1]
 17242 of 17242   100% in    0s     8.86 MB/s  done
upload: 'README.md' -> 's3://nnn/vB4yvlo5gh6XWXDa6vR9K6klxqxP7upf94ert8ncT5a8GJ5BPCQSvK0Jc1Jxyp9U8Ek643yFJ9MFFAdwv8TP7UynvGWvUI94nRwkpil9zpgNFxbEja237kklSwiVc9DulCLDXF25YwBFWuHINNVvU27fMy56l4vgoxyNhdKYRg4L45Bq5gyluRFSgMIuhOk3xcQqAIG62g4DB0Tex5q8Ap0JOOHIlm3jNBYVf2QcFVVBOZefsQeggyMjya1NVsTmDaYLRK0oUR970FV8BAWHo05yQW4S11FkPmAy8WbsaQqKtmO2qjJlnnflZ3ItE1Nt13Tb4ZPdVzm5Ev5DQiUGgHb9kgLXa43gsIy1sc1mWbB5PkDVoKpOHCXNaHsjKXBZ0JlvoGYh7PrJ5GhMcsxjbZqccoBsSe0u7tPesn4Zfvvx754VRmBD1W8MzQel2YmoDRLj3k2992odyWxp0zke68PpulRuLZIGQGSecSOivfyouqMz2YQ7lWl5hod0qGVN4VZTtCgSaScntOmpy1aAYDxNDm5uI601JtMIHMpXMIxyndYPAel0PH4HzFQ4dBiQDERWogoHRN4eU1vflZFpOesdyxIYhcR6VgZI9JFDdKlAZwzSv136UJ876lD4I1DEqGL03dLmEsitgn2uNE515Fa3srEbeb5KMWDQ2dfTUPr6NJEqvPZrObFJdRgrk2eWeajFWjl7TtxwgyjuqMye320R3z11SAaVoOa66As4ZHGz7YsDo3lSfdyFA2tFdvLIVvLEzpKWhoZyV8NATTuWYfDKotJU7LlvfDfXqsq25A6ycMYcGQn6i3n74Xo1Xpd8Ah88jWF8KCVviLr929Qy5UJTAQAwPUMFedW5ckrWPPX4Lc1Z7j7F0Zhf6vvKdgTX7WtzqFT4OmrLtbMXiSVSAUL3NELa9sc4DRScZvfpT11UMnAz0n8X8vjnaFUDPmwRuEcaO99tPN8g7fpLiMJYQXv1VCRzJlkrmipwCqc275mcvUOkBEx189DJ3Sq0zwIi'  [1 of 1]
 17242 of 17242   100% in    0s    16.28 MB/s  done

#
# 3. List objects in the bucket
#
$ s3cmd ls s3://mybucket
2025-04-09 09:45        17242  s3://mybucket/LWGKqSkJdRsN5ZoxKw3Z3TmfS7qM5OM54aPloJOTmaYzmswzf4LB5ARrWsyD3BXZsItCaKY5lTJzCb3XlAoBMdbymH875g7XCDgNYcqSEvLi14E3cTeQe4bzD1lrn3ZdDRubQuR3oCHHMRH6ny8R75s09rcVz1jlEiXLYuna7VmdCVVTFqUG6uDBV1pQZ8onAmOVm49Osr5GNWdjp0jP5e2AMpXodyzvlI3UISCu6QJYahDsmbMzCkeDqjm0qcX9xB9wdN28WijpDdgNB40SOsR21BOKhubgXxpKDP39Aj3oQaKz3zjXKNkkIvrtLLmq41qEKgbSWc8iZk2IZOjzFJcmi4G42mEy8t5eagKHylCyuxVfz72B9fRv7MaNJllLHKrZernHjQhHXu3RgoAdibx5qsoKTmZYTxeZNsBWn8IBE4oGrsGc99t638ETO7NBEnPW9Ibgxv0bpdwmfEwgzsvxmVrlAySuoBBuvYmL7KbY0PPBHtDe4gVfYYAg15Ba
2025-04-09 09:54        17242  s3://mybucket/vB4yvlo5gh6XWXDa6vR9K6klxqxP7upf94ert8ncT5a8GJ5BPCQSvK0Jc1Jxyp9U8Ek643yFJ9MFFAdwv8TP7UynvGWvUI94nRwkpil9zpgNFxbEja237kklSwiVc9DulCLDXF25YwBFWuHINNVvU27fMy56l4vgoxyNhdKYRg4L45Bq5gyluRFSgMIuhOk3xcQqAIG62g4DB0Tex5q8Ap0JOOHIlm3jNBYVf2QcFVVBOZefsQeggyMjya1NVsTmDaYLRK0oUR970FV8BAWHo05yQW4S11FkPmAy8WbsaQqKtmO2qjJlnnflZ3ItE1Nt13Tb4ZPdVzm5Ev5DQiUGgHb9kgLXa43gsIy1sc1mWbB5PkDVoKpOHCXNaHsjKXBZ0JlvoGYh7PrJ5GhMcsxjbZqccoBsSe0u7tPesn4Zfvvx754VRmBD1W8MzQel2YmoDRLj3k2992odyWxp0zke68PpulRuLZIGQGSecSOivfyouqMz2YQ7lWl5hod0qGVN4VZTtCgSaScntOmpy1aAYDxNDm5uI601JtMIHMpXMIxyndYPAel0PH4HzFQ4dBiQDERWogoHRN4eU1vflZFpOesdyxIYhcR6VgZI9JFDdKlAZwzSv136UJ876lD4I1DEqGL03dLmEsitgn2uNE515Fa3srEbeb5KMWDQ2dfTUPr6NJEqvPZrObFJdRgrk2eWeajFWjl7TtxwgyjuqMye320R3z11SAaVoOa66As4ZHGz7YsDo3lSfdyFA2tFdvLIVvLEzpKWhoZyV8NATTuWYfDKotJU7LlvfDfXqsq25A6ycMYcGQn6i3n74Xo1Xpd8Ah88jWF8KCVviLr929Qy5UJTAQAwPUMFedW5ckrWPPX4Lc1Z7j7F0Zhf6vvKdgTX7WtzqFT4OmrLtbMXiSVSAUL3NELa9sc4DRScZvfpT11UMnAz0n8X8vjnaFUDPmwRuEcaO99tPN8g7fpLiMJYQXv1VCRzJlkrmipwCqc275mcvUOkBEx189DJ3Sq0zwIi

#
# 4. Get object metadata
#
$ s3cmd info s3://mybucket/$longname
s3://nnn/vB4yvlo5gh6XWXDa6vR9K6klxqxP7upf94ert8ncT5a8GJ5BPCQSvK0Jc1Jxyp9U8Ek643yFJ9MFFAdwv8TP7UynvGWvUI94nRwkpil9zpgNFxbEja237kklSwiVc9DulCLDXF25YwBFWuHINNVvU27fMy56l4vgoxyNhdKYRg4L45Bq5gyluRFSgMIuhOk3xcQqAIG62g4DB0Tex5q8Ap0JOOHIlm3jNBYVf2QcFVVBOZefsQeggyMjya1NVsTmDaYLRK0oUR970FV8BAWHo05yQW4S11FkPmAy8WbsaQqKtmO2qjJlnnflZ3ItE1Nt13Tb4ZPdVzm5Ev5DQiUGgHb9kgLXa43gsIy1sc1mWbB5PkDVoKpOHCXNaHsjKXBZ0JlvoGYh7PrJ5GhMcsxjbZqccoBsSe0u7tPesn4Zfvvx754VRmBD1W8MzQel2YmoDRLj3k2992odyWxp0zke68PpulRuLZIGQGSecSOivfyouqMz2YQ7lWl5hod0qGVN4VZTtCgSaScntOmpy1aAYDxNDm5uI601JtMIHMpXMIxyndYPAel0PH4HzFQ4dBiQDERWogoHRN4eU1vflZFpOesdyxIYhcR6VgZI9JFDdKlAZwzSv136UJ876lD4I1DEqGL03dLmEsitgn2uNE515Fa3srEbeb5KMWDQ2dfTUPr6NJEqvPZrObFJdRgrk2eWeajFWjl7TtxwgyjuqMye320R3z11SAaVoOa66As4ZHGz7YsDo3lSfdyFA2tFdvLIVvLEzpKWhoZyV8NATTuWYfDKotJU7LlvfDfXqsq25A6ycMYcGQn6i3n74Xo1Xpd8Ah88jWF8KCVviLr929Qy5UJTAQAwPUMFedW5ckrWPPX4Lc1Z7j7F0Zhf6vvKdgTX7WtzqFT4OmrLtbMXiSVSAUL3NELa9sc4DRScZvfpT11UMnAz0n8X8vjnaFUDPmwRuEcaO99tPN8g7fpLiMJYQXv1VCRzJlkrmipwCqc275mcvUOkBEx189DJ3Sq0zwIi (object):
   File size: 17242
   Last mod:  Wed, 09 Apr 2025 09:54:30 GMT
   MIME type: none
   Storage:   STANDARD
   MD5 sum:   2940e853a9b97580df19b76d10d3ea31
   SSE:       none
   Policy:    none
   CORS:      none
   ACL:       none
   x-amz-meta-ais-cksum-type: md5
   x-amz-meta-ais-cksum-val: 2940e853a9b97580df19b76d10d3ea31

#
# 5. Confirm MD5
#
$ md5sum README.md
2940e853a9b97580df19b76d10d3ea31  README.md

#
# 6. Download the object with the long name
#
$ s3cmd get s3://mybucket/$longname ./downloaded-file.md
```

## Example: Using Native API

```console
#
# 1. Upload a file with an extremely long object name
#
$ ais put README.md mybucket/$longname

#
# 2. List objects with a specific prefix
#
$ ais ls ais://mybucket/$longname
PROPERTY         VALUE
atime            09 Apr 25 09:54 EDT
checksum         md5[2940e853a9b97580...]
name             ais://nnn/.x9a4cca2da6fffe8cd9aa78442e66f1c2f722caa28293024ddcce5b25c162586c
size             16.84KiB

#
# 3. Get object properties
#
$ ais show mybucket/$longname

#
# 4. Download the object
#
$ ais get mybucket/$longname ./downloaded-file.md
```

## Features

- **No Practical Length Limits**: AIStore can handle object names that are 4000+ characters long
- **Dual API Support**: Access the same capabilities through either native API or S3 API
- **Full Path Preservation**: Deep directory structures are maintained as-is
- **Consistent Performance**: Long object names do not impact operational speed

## References

- [_No limitations_ principle](/docs/overview.md#no-limitations-principle)
