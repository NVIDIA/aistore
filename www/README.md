# Download Manager
## Download Request
There are three types of Download Requests that DFC is able to accommodate: Single, Multi and List. 

Note: 
- When issuing a download request, ensure that the local bucket exists before issuing the request.
- To make multi and and list download requests, ensure that multiple networks are deployed when starting DFC.
  
### Single Object Download
As the title suggests, this should be used to download a single object from the internet. 

Single file download request are redirected to the correct Target (determined by the HRW algorithm)and then processed by the Target's Download Manager. 
Note the Download Manager dispatches the download to the request's corresponding mountpath jogger, where it is queued to be downloaded one-by-one.

#### Request Body Parameters
Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**link** | **string** | URL of where the object will be downloaded from. | 
**bucket** | **string** | bucket where the download will be saved to | 
**objname** | **string** | name of the object the download will be saved as. If no objname is provided, then the objname will be the last element in the URL&#39;s path. | Yes 
**headers** | **JSON object** | JSON object where the key is a header name and the value is the corresponding header value (string). These values are used as the header values for when DFC actually makes the GET request to download the object from the link. | Yes 

#### Sample Request
| Operation | HTTP action  | Example  | Notes |
|--|--|--|--|
| Single Object Download | POST /v1/download/single | `curl -L -i -v -X POST -H 'Content-Type: application/json' -d '{"bucket": "ubuntu", "objname": "ubuntu.iso", "headers":  {  "Authorization": "Bearer AbCdEf123456" }, "link": "http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso"}' http://localhost:8080/v1/download/single`| Header authorization is not required to make this download request. It is just provided as an example. |

### Multi Object Download
A multi object download request should be made to download multiple objects using one HTTP request. Multi Object Downloads require either an objectMap or an objectList. Provide an objectMap if you would like to customize the name of the download.

Multi download requests are first processed by the Proxy. The Proxy then issues a single file download requests in the background to the corresponding Target (determined by the HRW algorithm) for each object that needs to be downloaded.

#### Request Body Parameters
Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | bucket where the downloaded objects will be saved to | 
**headers** | **object** | JSON object where the key is a header name and the value is the corresponding header value(string). These values are used as the header values for when DFC actually makes the GET request to download the object. | Yes 
**objectMap** | **JSON object** | JSON object where the key is a URL (string) pointing to some file and the corresponding value (string) is the objname the object will be saved as. | Yes 
**objectList** | **JSON array** | JSON array where each item is a URL (string) pointing to some file. The objname for each file will be the last element in the URL&#39;s path. | Yes 

#### Sample Request
| Operation | HTTP action | Example |
|--|--|--|
| Multi Download Using Object Map | POST /v1/download/multi | `curl -L -i -v -X POST -H 'Content-Type: application/json' -d '{ "objectMap": { "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz": "t10k-images-idx3-ubyte.gz", "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz": "t10k-labels-idx1-ubyte.gz", "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz": "train-images-idx3-ubyte.gz"}, "bucket": "test321"} ' http://localhost:8080/v1/download/multi` |
| Multi Download Using Object List |  POST /v1/download/multi  | `curl -L -i -v -X POST -H 'Content-Type: application/json' -d '{"download_action": "download_multi",   "objectList": [   "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz",     "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz",      "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz" ], "bucket": "test321"} ' http://localhost:8080/v1/download/multi` |

### List Object Download
List download requests should be made to retrieve multiple files from servers that have files labelled in a list format. List download requests use prefix + index + suffix to form the objname for each object in the list.

List download requests are first processed by the Proxy. The Proxy then issues a single file download requests in the background to the corresponding Target (determined by the HRW algorithm) for each object that needs to be downloaded.

#### List Format
Consider a website like randomwebsite.com/somedirectory/ that contains the following files :
- object1log.txt
- object2log.txt
- object3log.txt
- ...
- object1000log.txt  
  
To populate DFC with object200log.txt to object300log.txt (101 files), it would be appropriate to use a list object download request instead of making 101 single download requests or populating the enormous object list or object map request body in a multi download request. 

#### Request Body Parameters
Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | bucket where the downloaded objects will be saved to | 
**headers** | **JSON object** | JSON object where the key is a header name and the value is the corresponding header value (string). These values are used as the header values for when DFC actually makes the GET request to download each object. | Yes 
**base** | **string** | The base URL of the object that will be used to formulate the download url | 
**prefix** | **string** | Is the first thing appended to the base string to formulate the download url | Yes 
**suffix** | **string** | the suffix follows the object index to formulate the download url of the object being downloaded. | Yes 
**start** | **int** | The index of the first object in the object space to be downloaded. Defualt is 0 if not provided. | Yes 
**end** | **int** | The upper bound of the range of objects to be downloaded in the object space. Defualt is 0 if not provided. | Yes 
**step** | **int** | Used to download every nth object (where n &#x3D; step) in the object space starting from start and ending at end. Default is 1 if not provided. | Yes 
**digit_count** | **int** | Used to ensure that each key coforms to n digits (where n &#x3D; digitCount). Basically prepends as many 0s as needed. i.e. if n &#x3D;&#x3D; 4, then the key 45 will be 0045 and if n &#x3D;&#x3D; 5, then key 45 wil be 00045. Not providing this field will mean no 0s are prepended to any index in the key space. | Yes  

#### Sample Request
| Operation | HTTP action | Example |
|--|--|--|
| Download a List of Objects | POST /v1/download/list | `curl -L -i -v -X POST -H 'Content-Type: application/json' -d '{"download_action": "download_list", "bucket": "test321",  "base": "randomwebsite.com/somedirectory/",  "prefix": "object",  "suffix": "log.txt",  "start": 200,  "end": 300, "step": 1, "digitCount": 0 }' http://localhost:8080/v1/download/list`|

## Cancel Request
After an initial download request is made to DFC, it may be cancelled by making a DELETE request to the Download endpoint. Downloader will periodically notify all of its joggers to check if their current object being downloaded has been cancelled.
Note download cancellations are not immediate.  It is possible for a cancel request to be issued but the download completes before the download cancel request is actually processed.

#### Request Body Parameters
Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**link** | **string** | URL of the object that was added to the download queue | 
**bucket** | **string** | bucket where the download was supposed to be saved to | 
**objname** | **string** | name of the object the download was supposed to be saved as | 

#### Sample Request
| Operation | HTTP action  | Example |
|--|--|--|
| Cancel Download | DELETE v1/download | `curl -L -i -v -X DELETE -H 'Content-Type: application/json' -d '{"bucket": "ubuntu", "objname": "ubuntu.iso", "link": "http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso"}' http://localhost:8080/v1/download`|

### Status Request
After an initial download request is made to DFC, its current progress can be queried using a status request.

#### Request Body Parameters
Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**link** | **string** | URL of the object that was added to the download queue | 
**bucket** | **string** | bucket where the download was supposed to be saved to | 
**objname** | **string** | name of the object the download was supposed to be saved as | 

#### Sample Request
| Operation | HTTP action | Example |
|--|--|--|
| Get Download Status | GET /v1/download/ | `curl -L -i -v -X GET -H 'Content-Type: application/json' -d '{"bucket": "ubuntu", "objname": "ubuntu.iso", "link": "http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso"}' http://localhost:8080/v1/download`|