# openapi_client.BucketApi

All URIs are relative to *http://localhost:8080/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete**](BucketApi.md#delete) | **DELETE** /buckets/{bucket-name} | Delete operations on bucket and its contained objects
[**get_properties**](BucketApi.md#get_properties) | **HEAD** /buckets/{bucket-name} | Query bucket properties
[**list_names**](BucketApi.md#list_names) | **GET** /buckets/* | Get bucket names
[**perform_operation**](BucketApi.md#perform_operation) | **POST** /buckets/{bucket-name} | Perform operations on bucket such as create
[**set_properties**](BucketApi.md#set_properties) | **PUT** /buckets/{bucket-name} | Set bucket properties


# **delete**
> delete(bucket_name, input_parameters)

Delete operations on bucket and its contained objects

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.BucketApi()
bucket_name = 'bucket_name_example' # str | Bucket name
input_parameters = openapi_client.InputParameters() # InputParameters | 

try:
    # Delete operations on bucket and its contained objects
    api_instance.delete(bucket_name, input_parameters)
except ApiException as e:
    print("Exception when calling BucketApi->delete: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bucket_name** | **str**| Bucket name | 
 **input_parameters** | [**InputParameters**](InputParameters.md)|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_properties**
> get_properties(bucket_name)

Query bucket properties

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.BucketApi()
bucket_name = 'bucket_name_example' # str | Bucket name

try:
    # Query bucket properties
    api_instance.get_properties(bucket_name)
except ApiException as e:
    print("Exception when calling BucketApi->get_properties: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bucket_name** | **str**| Bucket name | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_names**
> BucketNames list_names(local=local)

Get bucket names

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.BucketApi()
local = True # bool | Get only local bucket names (optional)

try:
    # Get bucket names
    api_response = api_instance.list_names(local=local)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling BucketApi->list_names: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **local** | **bool**| Get only local bucket names | [optional] 

### Return type

[**BucketNames**](BucketNames.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/jsontext/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **perform_operation**
> ObjectPropertyList perform_operation(bucket_name, input_parameters)

Perform operations on bucket such as create

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.BucketApi()
bucket_name = 'bucket_name_example' # str | Bucket name
input_parameters = openapi_client.InputParameters() # InputParameters | 

try:
    # Perform operations on bucket such as create
    api_response = api_instance.perform_operation(bucket_name, input_parameters)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling BucketApi->perform_operation: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bucket_name** | **str**| Bucket name | 
 **input_parameters** | [**InputParameters**](InputParameters.md)|  | 

### Return type

[**ObjectPropertyList**](ObjectPropertyList.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/jsontext/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_properties**
> set_properties(bucket_name, input_parameters, cloud_provider=cloud_provider, next_tier_url=next_tier_url, read_policy=read_policy, write_policy=write_policy)

Set bucket properties

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.BucketApi()
bucket_name = 'bucket_name_example' # str | Bucket name
input_parameters = openapi_client.InputParameters() # InputParameters | 
cloud_provider = 'cloud_provider_example' # str | Bucket's cloud provider (optional)
next_tier_url = 'next_tier_url_example' # str | URL for the next tier (optional)
read_policy = openapi_client.RWPolicy() # RWPolicy | Policy which defines how to perform reads in case of more tiers (optional)
write_policy = openapi_client.RWPolicy() # RWPolicy | Policy which defines how to perform writes in case of more tiers (optional)

try:
    # Set bucket properties
    api_instance.set_properties(bucket_name, input_parameters, cloud_provider=cloud_provider, next_tier_url=next_tier_url, read_policy=read_policy, write_policy=write_policy)
except ApiException as e:
    print("Exception when calling BucketApi->set_properties: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bucket_name** | **str**| Bucket name | 
 **input_parameters** | [**InputParameters**](InputParameters.md)|  | 
 **cloud_provider** | **str**| Bucket&#39;s cloud provider | [optional] 
 **next_tier_url** | **str**| URL for the next tier | [optional] 
 **read_policy** | [**RWPolicy**](.md)| Policy which defines how to perform reads in case of more tiers | [optional] 
 **write_policy** | [**RWPolicy**](.md)| Policy which defines how to perform writes in case of more tiers | [optional] 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

