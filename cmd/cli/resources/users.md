# User Account and Access management

[AuthN](/cmd/authn/README.md) is AIS authorization server that can be deployed to manage user access to one or more AIS clusters.

The CLI provides an easy way to manage users and to grant and revoke access permissions.

If the AIS cluster does not have guest access enabled, every user that needs access to the cluster data must be registered. Guest access allows unregistered users to use the AIS cluster in read-only mode.

When a token is revoked or a user with valid issued tokens is removed, AuthN notifies registered clusters, so they update their blacklists.

## Command List

All commands (except logout) send requests to AuthN URL defined in AIS CLI configuration file. Configuration can be overridden with environment variable `AUTHN_URL`, e.g., `AUTHN_URL=http://10.0.0.20:52001 ais auth add ...`.

Adding and removing a user requires superuser permissions. Superuser login and password can be provided in command line:

`AUTHN_SU_NAME=admin AUTHN_SU_PASS=admin ais auth add ...`

or you can set environment variables globally with:

```console
$ export AUTHN_SU_NAME=admin
$ export AUTHN_SU_PASS=admin
$ ais auth add ...
```

or just run:

`ais auth add..`

In the last case, the CLI prompts for missing data in interactive mode. E.g, you can keep superuser name in an environment variable and `ais` will prompt for superuser's password:

```console
$ export AUTHN_SU_NAME=admin
$ ais auth add ...
Superuser password:
```

## Register new user

`ais auth add user [USER_NAME USER_PASS] [--role ROLE]`

Register the user and grant `role` permissions to the user.
For security reasons, user's password can be omitted (and user's name as well).
In this case, the CLI prompts for every missing argument in interactive mode.

Option `--role` sets the user's default permissions to access the cluster.
If the option it omitted, the user gets role `Guest` that allows only read-only access.

**Examples:**

```console
$ ais auth add user user1 password
Superuser password: admin
User password: password
$ ais auth add user user2 password --role PowerUser
Superuser password: admin
User password: password
$ ais auth show user
NAME    ROLE       PERMISSIONS
user2   PowerUser  18446744073709551615
guest   Guest      771
user1   Guest      771
```

## Unregister existing user

`ais auth rm user USER_NAME`

Remove an existing user and revokes all tokens issued for the user.

## List registered users

`ais auth show user`

Displays the list of registered users. The list is alphabetically sorted and
divided into three groups:

- First group is PowerUser group
- Seconds group is a list of users that have restricted write access
- The last group are Guest group - list of read-only users

```console
$ ais auth show user
NAME    ROLE       PERMISSIONS
user2   PowerUser  18446744073709551615
guest   Guest      771
user1   Guest      771
```

## List existing roles

`ais auth show role`

Displays existing roles in alphabetical order.

```console
$ ais auth show role
ROLE            DESCRIPTION
BucketOwner     Full access to buckets
Guest           Read-only access to buckets
PowerUser       Full access to cluster
```

## Log in to AIS cluster

`ais auth login USER_NAME USER_PASS`

Issue a token for a user.
After successful login, user's token is saved to `~/.ais/token` and next CLI runs automatically load and use the token in every request to AIS cluster.
The saved token can be used by other applications, like `curl`.
Please see [AuthN documentation](/cmd/authn/README.md) to read how to use AuthN API directly.

## Log out

`ais auth logout`

Erase user's token from a local machine, so the CLI switches to read-only mode (if guest access is enabled for AIS cluster).
But other applications still can use the issued token.
To forbid using the token from any application, the token must be revoked in addition to logging out.

## Register new cluster

`ais auth add cluster CLUSTER_ID=URL[,URL..]`

Register the cluster by its ID or alias and assign the list URLs for sending notifications.
AuthN sends only one notification per cluster.

## Update existing cluster URL list

`ais auth update cluster CLUSTER_ID=URL[,URL..]`

Replaces the list of URLs for an existing cluster.

## Unregister existing cluster

`ais auth rm cluster CLUSTER_ID`

Remove the existing cluster from notification list.

## List registered clusters

`ais auth show cluster`

Display the list of the cluster that subscribe to AuthN notifications.

```console
$ ais auth add cluster srv0=http://172.0.10.10,http://10.0.10.10
$ ais auth show cluster
ClusterID       URLs
srv0            http://172.0.10.10,http://10.0.10.10
```
