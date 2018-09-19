# openapi_client.ObjectApi

All URIs are relative to *http://localhost:8080/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete**](ObjectApi.md#delete) | **DELETE** /objects/{bucket-name}/{object-name} | Delete object
[**get**](ObjectApi.md#get) | **GET** /objects/{bucket-name}/{object-name} | Get object
[**get_properties**](ObjectApi.md#get_properties) | **HEAD** /objects/{bucket-name}/{object-name} | Query object properties
[**perform_operation**](ObjectApi.md#perform_operation) | **POST** /objects/{bucket-name}/{object-name} | Perform operations on object such as rename
[**put**](ObjectApi.md#put) | **PUT** /objects/{bucket-name}/{object-name} | Put object


# **delete**
> delete(bucket_name, object_name, input_parameters=input_parameters)

Delete object

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.ObjectApi()
bucket_name = 'bucket_name_example' # str | Bucket name
object_name = 'object_name_example' # str | Object name
input_parameters = openapi_client.InputParameters() # InputParameters |  (optional)

try:
    # Delete object
    api_instance.delete(bucket_name, object_name, input_parameters=input_parameters)
except ApiException as e:
    print("Exception when calling ObjectApi->delete: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bucket_name** | **str**| Bucket name | 
 **object_name** | **str**| Object name | 
 **input_parameters** | [**InputParameters**](InputParameters.md)|  | [optional] 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get**
> str get(bucket_name, object_name, offset=offset, length=length)

Get object

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.ObjectApi()
bucket_name = 'bucket_name_example' # str | Bucket name
object_name = 'object_name_example' # str | Object name
offset = 56 # int | Starting byte from where the read needs to be performed (optional)
length = 56 # int | Number of bytes that need to be returned starting from the offset (optional)

try:
    # Get object
    api_response = api_instance.get(bucket_name, object_name, offset=offset, length=length)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling ObjectApi->get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bucket_name** | **str**| Bucket name | 
 **object_name** | **str**| Object name | 
 **offset** | **int**| Starting byte from where the read needs to be performed | [optional] 
 **length** | **int**| Number of bytes that need to be returned starting from the offset | [optional] 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/octet-stream, text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_properties**
> get_properties(bucket_name, object_name, check_cached=check_cached)

Query object properties

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.ObjectApi()
bucket_name = 'bucket_name_example' # str | Bucket name
object_name = 'object_name_example' # str | Object name
check_cached = True # bool | Check if the object is cached (optional)

try:
    # Query object properties
    api_instance.get_properties(bucket_name, object_name, check_cached=check_cached)
except ApiException as e:
    print("Exception when calling ObjectApi->get_properties: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bucket_name** | **str**| Bucket name | 
 **object_name** | **str**| Object name | 
 **check_cached** | **bool**| Check if the object is cached | [optional] 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **perform_operation**
> perform_operation(bucket_name, object_name, input_parameters)

Perform operations on object such as rename

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.ObjectApi()
bucket_name = 'bucket_name_example' # str | Bucket name
object_name = 'object_name_example' # str | Object name
input_parameters = openapi_client.InputParameters() # InputParameters | 

try:
    # Perform operations on object such as rename
    api_instance.perform_operation(bucket_name, object_name, input_parameters)
except ApiException as e:
    print("Exception when calling ObjectApi->perform_operation: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bucket_name** | **str**| Bucket name | 
 **object_name** | **str**| Object name | 
 **input_parameters** | [**InputParameters**](InputParameters.md)|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **put**
> put(bucket_name, object_name, from_id=from_id, to_id=to_id, body=body)

Put object

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.ObjectApi()
bucket_name = 'bucket_name_example' # str | Bucket name
object_name = 'object_name_example' # str | Object name
from_id = 'from_id_example' # str | Source target ID (optional)
to_id = 'to_id_example' # str | Destination target ID (optional)
body = '/path/to/file' # file |  (optional)

try:
    # Put object
    api_instance.put(bucket_name, object_name, from_id=from_id, to_id=to_id, body=body)
except ApiException as e:
    print("Exception when calling ObjectApi->put: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bucket_name** | **str**| Bucket name | 
 **object_name** | **str**| Object name | 
 **from_id** | **str**| Source target ID | [optional] 
 **to_id** | **str**| Destination target ID | [optional] 
 **body** | **file**|  | [optional] 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/octet-stream
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

