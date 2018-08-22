# openapi_client.SortApi

All URIs are relative to *http://localhost:8080/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**abort_sort**](SortApi.md#abort_sort) | **DELETE** /sort/abort/{sort-uuid} | Abort distributed sort operation
[**get_sort_metrics**](SortApi.md#get_sort_metrics) | **GET** /sort/metrics/{sort-uuid} | Get metrics of given sort operation
[**start_sort**](SortApi.md#start_sort) | **POST** /sort/start | Starts distributed sort operation on cluster


# **abort_sort**
> abort_sort(sort_uuid)

Abort distributed sort operation

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.SortApi()
sort_uuid = 'sort_uuid_example' # str | Sort uuid which is returned when starting dsort

try:
    # Abort distributed sort operation
    api_instance.abort_sort(sort_uuid)
except ApiException as e:
    print("Exception when calling SortApi->abort_sort: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sort_uuid** | [**str**](.md)| Sort uuid which is returned when starting dsort | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_sort_metrics**
> dict(str, object) get_sort_metrics(sort_uuid)

Get metrics of given sort operation

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.SortApi()
sort_uuid = 'sort_uuid_example' # str | Sort uuid which is returned when starting dsort

try:
    # Get metrics of given sort operation
    api_response = api_instance.get_sort_metrics(sort_uuid)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling SortApi->get_sort_metrics: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sort_uuid** | [**str**](.md)| Sort uuid which is returned when starting dsort | 

### Return type

**dict(str, object)**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **start_sort**
> str start_sort(sort_spec)

Starts distributed sort operation on cluster

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.SortApi()
sort_spec = openapi_client.SortSpec() # SortSpec | 

try:
    # Starts distributed sort operation on cluster
    api_response = api_instance.start_sort(sort_spec)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling SortApi->start_sort: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sort_spec** | [**SortSpec**](SortSpec.md)|  | 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

