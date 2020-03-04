---
layout: post
title: API
permalink: api
redirect_from:
- api/README.md/
---

AIStore Client API
--------------

## Overview
The AIStore Client API package provides wrappers for core AIStore RESTful operations. The `api` package can be imported by other projects for quickly integrating AIStore functionality with minimal imports of other supporting packages.

## Types of Operations
The APIs provided are separated into different levels of granularity:

1. Cluster,
2. Daemon,
3. Bucket,
4. Object.

### **Cluster**

#### GetClusterMap
Retrieves AIStore's cluster map as stored by the proxy to which this request is sent

##### Parameters
| Name        | Type         | Description                                                                           |
|-------------|--------------|---------------------------------------------------------------------------------------|
|  httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL    | string       | URL of the proxy to which the HTTP Request is sent                                    |

##### Return
A copy of type `cluster.Smap` containing a map of targets, a map of proxies, proxies non-eligible for primary, the current primary proxy, and the version of the cluster map

Error from AIStore in completing the request
___
#### SetPrimaryProxy
Given a daemonID, it sets that proxy as the primary proxy of the cluster

##### Parameters
| Name         | Type         | Description                                                                           |
|--------------|--------------|---------------------------------------------------------------------------------------|
|  httpClient  | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL     | string       | URL of the proxy to which the HTTP Request is sent                                    |
| newPrimaryID | string       | DaemonID of the new primary proxy                                                     |
##### Return
Error from AIStore in completing the request
___
#### SetClusterConfig
Given key-value pairs of configuration parameters, this operation sets the cluster-wide configuration accordingly. Setting cluster-wide configuration requires sending the request to a proxy

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| nvs        | key-values   | Map of key-value pairs of configuration parameters to be set                          |

##### Return
Error from AIStore in completing the request
___

#### RegisterNode
Registers an existing node to the clustermap.

##### Parameters
| Name       | Type           | Description                                                                           |
|------------|----------------|---------------------------------------------------------------------------------------|
| httpClient |  *http.Client  | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string         | URL of the proxy to which the HTTP Request is sent                                    |
| nodeInfo   | *cluster.Snode | Pointer to a cluster.Snode struct containing details of the new node                  |

##### Return
Error from AIStore in completing the request
___

#### UnregisterNode
Unregisters an existing node from the clustermap.

##### Parameters
| Name          | Type         | Description                                                                           |
|---------------|--------------|---------------------------------------------------------------------------------------|
| httpClient    | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL      | string       | URL of the proxy to which the HTTP Request is sent                                    |
| unregisterSID | string       | DaemonID of the node to be unregistered                                               |

##### Return
Error from AIStore in completing the request
___

### **Daemon**

#### GetMountpaths
Given the direct public URL of a target, `GetMountpaths` returns its mountpaths

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| targetURL  | string       | URL of the target to which the HTTP Request is sent                                   |
##### Return
A pointer to an instance of a `cmn.MountpathList` struct consisting of Available and Disabled mountpaths of a specific target

Error from AIStore in completing the request
___

#### AddMountpath
Given a target and a mountpath, `AddMountpath` adds that mountpath to the specified target
##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| targetURL  | string       | URL of the target to which the HTTP Request is sent                                   |
| mountPath  | string       | Mountpath to be added to a target                                                     |
##### Return
Error from AIStore in completing the request
___

#### RemoveMountpath
Given a target and a mountpath, `RemoveMountpath` removes that mountpath from the specified target

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| targetURL  | string       | URL of the target to which the HTTP Request is sent                                   |
| mountPath  | string       | Mountpath to be removed from a target                                                 |

##### Return
Error from AIStore in completing the request
___

#### EnableMountpath
Given a target and a mountpath, `EnableMountpath` enables that mountpath on the specified target

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| targetURL  | string       | URL of the target to which the HTTP Request is sent                                   |
| mountPath  | string       | Mountpath to be enabled on a target                                                   |

##### Return
Error from AIStore in completing the request
___

#### DisableMountpath
Given a target and a mountpath, `DisableMountpath` disables that mountpath on the specified target

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| targetURL  | string       | URL of the target to which the HTTP Request is sent                                   |
| mountPath  | string       | Mountpath to be disabled on a target                                                  |
##### Return
Error from AIStore in completing the request
___

#### GetDaemonConfig
Given the URL of a daemon, `GetDaemonConfig` returns the corresponding daemon's configuration settings 
##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| daemonURL  | string       | URL of the daemon to which the HTTP Request is sent                                   |

##### Return
A pointer to an instance of type `cmn.Config` containing all the configuration settings applied to a specific daemon

Error from AIStore in completing the request
___

#### SetDaemonConfig
Given key-value pairs of configuration parameters, `SetDaemonConfig` sets the configuration accordingly for a specific daemon
##### Parameters
| Name       | Type              | Description                                                                           |
|------------|-------------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client      | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| daemonURL  | string            | URL of the daemon to which the HTTP Request is sent                                   |
| nvs        | key-values        | Map of key-value pairs of configuration parameters to be set                          |

##### Return
Error from AIStore in completing the request
___


### **Bucket**

#### HeadBucket
Given a bucket name, returns its properties

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string       | Name of the bucket                                                                    |
| query      | url.Values   | Optional URL query values such as [`?provider`](#url_query_values)                    |

##### Return
A pointer to an instance of `cmn.BucketProps`, consisting of all the properties of the specified bucket

Error from AIStore in completing the request
___

#### GetBucketNames
Given the url of an existing proxy in a cluster, `GetBucketNames` returns the names of all existing local and cloud buckets

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| provider   | string       | One of "" (empty), "cloud", "ais", "aws", "gcp". If the value is empty, returns all bucket names. Otherwise, returns only "cloud" or "ais" buckets.|

##### Return
Two lists: one for the names of ais buckets, and the other for the names of cloud buckets

Error from AIStore in completing the request
___

#### CreateLocalBucket
Creates an ais bucket with a given name

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string       | Name of the bucket                                                                    |
##### Return
Error from AIStore in completing the request
___

#### CopyLocalBucket
Create new bucket and copy into it all objects from the existing (old) one

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| oldName    | string       | Name of the existing bucket                                                           |
| newName    | string       | Name of the new bucket                                                                |

#### RenameLocalBucket
Rename an existing bucket to the new name provided

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| oldName    | string       | Name of the existing bucket                                                           |
| newName    | string       | New name for the existing bucket                                                      |

##### Return
Error from AIStore in completing the request
___

#### DestroyLocalBucket
Removes an ais bucket using its name as the identifier

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string       | Name of the existing bucket                                                           |

##### Return
Error from AIStore in completing the request
___

#### EvictCloudBucket
Evicts a cloud bucket using its name as the identifier

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string       | Name of the existing bucket                                                           |
| query      | url.Values   | Optional URL query values such as [`?provider`](#url_query_values)                    |
##### Return
Error from AIStore in completing the request
___

#### SetBucketProps
Sets the properties of a bucket via action message, using the bucket name as the identifier and the bucket properties to be set
> Need to set all bucket properties

##### Parameters
| Name       | Type            | Description                                                                           |
|------------|-----------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client    | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string          | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string          | Name of the existing bucket                                                           |
| props      | cmn.BucketProps | Bucket properties to be set                                                           |
| query      | url.Values      | Optional URL query values such as [`?provider`](#url_query_values)                    |

##### Return
Error from AIStore in completing the request
___

#### ResetBucketProps
Resets the properties of a bucket, identified by its name, to the global configuration

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string       | Name of the existing bucket                                                           |
| query      | url.Values   | Optional URL query values such as [`?provider`](#url_query_values)                    |

##### Return
Error from AIStore in completing the request
___


### **Object**

#### HeadObject
Returns the size and version of an object identified by a combination of its bucket and object names

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string       | Name of the bucket storing the object                                                 |
| provider   | string       | Cloud provider, one of: "", "cloud", "ais", "gcp", "aws". If not specified (""), AIS determines the value by checking bucket metadata |
| object     | string       | Name of the object                                                                    |

##### Return
A pointer of an instance of `cmn.ObjectProps`, containing information on the size and version of the object

Error from AIStore in completing the request
___

#### GetObject
Returns the size of the object. Does not validate checksum of the object in the response

Writes the response body to a writer if one is specified in the optional `GetObjectInput.Writer`
Otherwise, it discards the response body read.

##### Parameters
| Name       | Type           | Description                                                                           |
|------------|----------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client   | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string         | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string         | Name of the bucket storing the object                                                 |
| object     | string         | Name of the object                                                                    |
| options    | GetObjectInput | Optional field with a custom Writer and URL Query values                              |

##### Return
Size of the object computed from the number of bytes read

Error from AIStore in completing the request
___

#### GetObjectWithValidation
Same behavior as `GetObject`, but performs checksum validation of the object by comparing the checksum in the response header with the calculated checksum value derived from the returned object.

##### Parameters
| Name       | Type           | Description                                                                           |
|------------|----------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client   | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string         | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string         | Name of the bucket storing the object                                                 |
| object     | string         | Name of the object                                                                    |
| options    | GetObjectInput | Optional field with a custom Writer and URL Query values                              |

##### Return
Size of the object computed from the number of bytes read

Error from AIStore in completing the request
___

#### PutObject
Creates an object from the body of the `cmn.ReadOpenCloser` argument and puts it in the bucket identified by its name. The name of the object put is likewise identified by its name. If the object hash passed in is not empty, the value is set in the request header with the default checksum type "xxhash"
##### Parameters
| Name          | Type                 | Description                                                                           |
|---------------|----------------------|---------------------------------------------------------------------------------------|
| args          | PutObjectArgs        | A field that handles the arguments for PutObject                                      |
| replicateOpts | ReplicateObjectInput | Used to hold optional parameters for PutObject when it is used for replication        |

##### PutObjectArgs
| Name       | Type               | Description                                                                           |
|------------|--------------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client       | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string             | URL of the proxy to which the HTTP Request is sent                                    |
| Bucket     | string             | Name of the bucket storing the object                                                 |
| Provider   | string             | Cloud provider, one of: "", "cloud", "ais", "gcp", "aws". If not specified (""), AIS determines it by checking bucket metadata |
| Object     | string             | Name of the object                                                                    |
| Hash       | string             | Hash computed for the object                                                          |
| Reader     | cmn.ReadOpenCloser | Interface used to read the bytes of object data                                       |

##### ReplicateObjectInput
| Name       | Type               | Description                                                                                          |
|------------|--------------------|------------------------------------------------------------------------------------------------------|
| SourceURL  | string             | Used to set the request header to determine whether PUT object request is for replication in AIStore |

##### Return
Error from AIStore in completing the request
___

#### RenameObject
Renames an existing object

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string       | Name of the bucket storing the object                                                 |
| oldName    | string       | Name of the existing object                                                           |
| newName    | string       | New name for the existing object                                                      |

##### Return
Error from AIStore in completing the request
___

#### ReplicateObject
Replicates a given object

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string       | Name of the bucket storing the object                                                 |
| object     | string       | Name of the object to be replicated                                                   |

##### Return
Error from AIStore in completing the request
___

#### DeleteObject
Deletes an object identified by the combination of its bucket and object name

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string       | Name of the bucket storing the object                                                 |
| object     | string       | Name of the object to be replicated                                                   |
| provider   | string       | Cloud provider, one of "", "cloud", "ais". Other supported values include "aws" and "gcp", for Amazon and Google clouds, respectively |

##### Return
Error from AIStore in completing the request
___

#### EvictObject
Evicts an object identified by the combination of its bucket and object name

##### Parameters
| Name       | Type         | Description                                                                           |
|------------|--------------|---------------------------------------------------------------------------------------|
| httpClient | *http.Client | HTTP Client used to create and process the HTTP Request and return the HTTP Response  |
| proxyURL   | string       | URL of the proxy to which the HTTP Request is sent                                    |
| bucket     | string       | Name of the bucket storing the object                                                 |
| object     | string       | Name of the object to be evicted                                                      |
##### Return
Error from AIStore in completing the request
___


### Optional Parameters

**ParamsOptional**
| Name    | Type          | Description                                                                                           |
|---------|---------------|-------------------------------------------------------------------------------------------------------|
| Query   | url.Values    | Map of string keys to slice of strings values, used to set the URL.RawQuery field of the HTTP Request |
| Headers | http.Header   | Map of string keys to string values, used to set the HTTP Request header                              |
___

### URL Query Values
| Name | Fields | Description |
| --- | --- | --- |
| provider | "", "cloud", "ais" | Cloud provider - "cloud" or "ais". Other supported values include "gcp" and "aws", for Amazon and Google clouds, respectively. If omitted, provider of the bucket is determined by checking bucket metadata |

## Basic API Workflow
A sample demo of the APIs listed above:
```go
func demo() error {
	var (
        httpClient = &http.Client{}
        url = "http://localhost:8080"
        bucket = "DemoBucket"
        object = "DemoObject"
	)

    // Fetch cluster map
    smap, err := api.GetClusterMap(httpClient, url)
    if err != nil {
        return Errors.New("Getting clustermap failed, %v\n", err)
    }
 
    primaryProxyURL := smap.ProxySI.PublicNet.DirectURL
 
    // Create ais bucket
    err = api.CreateLocalBucket(httpClient, primaryProxyURL, bucket)
    if err != nil {
        return Errors.New("Creating ais bucket failed, %v\n", err)
    }
 
    newBucketName = "DemoBucketNew"
    // Rename ais bucket
    err = api.RenameLocalBucket(httpClient, primaryProxyURL, bucket, newBucketName)
    if err != nil {
        return Errors.New("Renaming ais bucket failed, %v\n", err)
    }
}
```
