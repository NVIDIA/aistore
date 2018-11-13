# openapi_client.ClusterApi

All URIs are relative to *http://localhost:8080/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get**](ClusterApi.md#get) | **GET** /cluster/ | Get cluster related details
[**perform_operation**](ClusterApi.md#perform_operation) | **PUT** /cluster/ | Perform cluster wide operations such as setting config value, shutting down proxy/target etc.
[**register_target**](ClusterApi.md#register_target) | **POST** /cluster/register/ | Register storage target
[**set_primary_proxy**](ClusterApi.md#set_primary_proxy) | **PUT** /cluster/proxy/{primary-proxy-id} | Set primary proxy
[**unregister_target**](ClusterApi.md#unregister_target) | **DELETE** /cluster/daemon/{daemonId} | Unregister the storage target


# **get**
> object get(what, props=props)

Get cluster related details

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.ClusterApi()
what = openapi_client.GetWhat() # GetWhat | Cluster details which need to be fetched
props = openapi_client.GetProps() # GetProps | Additional properties describing the cluster details (optional)

try:
    # Get cluster related details
    api_response = api_instance.get(what, props=props)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling ClusterApi->get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **what** | [**GetWhat**](.md)| Cluster details which need to be fetched | 
 **props** | [**GetProps**](.md)| Additional properties describing the cluster details | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **perform_operation**
> perform_operation(input_parameters)

Perform cluster wide operations such as setting config value, shutting down proxy/target etc.

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.ClusterApi()
input_parameters = openapi_client.InputParameters() # InputParameters | 

try:
    # Perform cluster wide operations such as setting config value, shutting down proxy/target etc.
    api_instance.perform_operation(input_parameters)
except ApiException as e:
    print("Exception when calling ClusterApi->perform_operation: %s\n" % e)
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

# **register_target**
> register_target(snode)

Register storage target

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.ClusterApi()
snode = openapi_client.Snode() # Snode | 

try:
    # Register storage target
    api_instance.register_target(snode)
except ApiException as e:
    print("Exception when calling ClusterApi->register_target: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **snode** | [**Snode**](Snode.md)|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_primary_proxy**
> set_primary_proxy(primary_proxy_id)

Set primary proxy

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.ClusterApi()
primary_proxy_id = 'primary_proxy_id_example' # str | Bucket name

try:
    # Set primary proxy
    api_instance.set_primary_proxy(primary_proxy_id)
except ApiException as e:
    print("Exception when calling ClusterApi->set_primary_proxy: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **primary_proxy_id** | **str**| Bucket name | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **unregister_target**
> unregister_target(daemon_id)

Unregister the storage target

### Example
```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = openapi_client.ClusterApi()
daemon_id = 'daemon_id_example' # str | ID of the target daemon

try:
    # Unregister the storage target
    api_instance.unregister_target(daemon_id)
except ApiException as e:
    print("Exception when calling ClusterApi->unregister_target: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **daemon_id** | **str**| ID of the target daemon | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

