# openapi_client.DaemonApi

All URIs are relative to *http://localhost:8080/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**perform_operation**](DaemonApi.md#perform_operation) | **PUT** /daemon/ | Perform operations such as setting config value, shutting down proxy/target etc. on a DFC daemon


# **perform_operation**
> perform_operation(input_parameters)

Perform operations such as setting config value, shutting down proxy/target etc. on a DFC daemon

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.DaemonApi()
input_parameters = openapi_client.InputParameters() # InputParameters | 

try:
    # Perform operations such as setting config value, shutting down proxy/target etc. on a DFC daemon
    api_instance.perform_operation(input_parameters)
except ApiException as e:
    print("Exception when calling DaemonApi->perform_operation: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **input_parameters** | [**InputParameters**](InputParameters.md)|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

