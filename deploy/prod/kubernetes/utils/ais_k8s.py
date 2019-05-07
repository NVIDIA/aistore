#
# Run with python2 - a requirement of the AIS openapi_client
#
# Python script to query/validate AIS cluster state & health on k8s as deployed
# by our Helm chart.
#
# This scripts uses hard-coded knowledge of labels etc that are used/expected by
# the AIS helm chart. It should instead query those labels from the DaemonSets
# in use, and learn dynamically.
#
# Because this script needs to contact the individual daemon endpoints, it must be
# run on a k8s cluster node or from within a pod in the cluster.
#
# This script uses the ~/.kube/config mechanism for authentication.
#

from __future__ import print_function
import datetime
import kubernetes.client
import openapi_client
import os
import ptvsd
import pytz
import sys
import threading
import time

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from pprint import pprint
from operator import itemgetter, attrgetter
from pprint import pprint
from urllib3.exceptions import MaxRetryError, NewConnectionError

if os.getenv('REMOTE_DEBUG_LOCAL_IP'):
    ptvsd.enable_attach(address=(os.getenv('REMOTE_DEBUG_LOCAL_IP'), os.getenv('REMOTE_DEBUG_LOCAL_PORT')), redirect_output=True)
    ptvsd.wait_for_attach()

class Ais:
    """Kitchen sink class for AIS validation on k8s
    """

    #
    # Node label selectors
    #
    nodeProxyLabel = 'ais-proxy-type=electable'
    nodeNeProxyLabel = 'ais-proxy-type=nonelectable'
    nodeTargetLabel = 'ais-target-node=yes'

    #
    # Pod label selectors
    #
    podProxyLabel = "app=ais,component=proxy"
    podNeProxyLabel = "app=ais,component=ne_proxy"
    podTargetLabel = "app=ais,component=target"

    openapi_models = openapi_client.models
    openapi_params = openapi_models.InputParameters
    openapi_actions = openapi_models.Actions

    #
    # Proxy clusterIP service selector
    #
    proxySvcLabel = "app=ais"

    def __init__(self):
        """ Load current kube config context and initialize API handles we require. """

        config.load_kube_config()
        self.v1api = client.CoreV1Api()

        self.refreshAisK8sState()
        self.refreshAisDaemonState()
    
    def refreshAisK8sState(self, quiet=False):
        """ Query k8s for AIS state. """

        #
        # Load nodes labelled as per nodeSelectors for proxy, ne-proxy, target
        #
        if not quiet:
            print("Querying AIS k8s cluster nodes ...")
        self.nodes_proxy = sorted(self.v1api.list_node(label_selector=self.nodeProxyLabel).items, key=attrgetter('metadata.name'))
        self.nodes_neproxy = sorted(self.v1api.list_node(label_selector=self.nodeNeProxyLabel).items, key=attrgetter('metadata.name'))
        self.nodes_target = sorted(self.v1api.list_node(label_selector=self.nodeTargetLabel).items, key=attrgetter('metadata.name'))

        #
        # Look for node labelled as initial primary proxy
        #
        for node in self.nodes_proxy:
            if node.metadata.labels.get(u'initial_primary_proxy', None) == 'yes':
                self.initialPrimaryNodeName = node.metadata.name
                break
        else:
            self.initialPrimaryNodeName = None

        #
        # Load pods with our labels, allowing for uninitialized pods
        #
        if not quiet:
            print("Querying AIS k8s pods ...")
        proxy_pods = sorted(self.v1api.list_pod_for_all_namespaces(label_selector=self.podProxyLabel, include_uninitialized=True).items, key=attrgetter('spec.node_name'))
        ne_proxy_pods = sorted(self.v1api.list_pod_for_all_namespaces(label_selector=self.podNeProxyLabel, include_uninitialized=True).items, key=attrgetter('spec.node_name'))
        target_pods = sorted(self.v1api.list_pod_for_all_namespaces (label_selector=self.podTargetLabel, include_uninitialized=True).items, key=attrgetter('spec.node_name'))
        
        #
        # Each AIS Daemon will have a dict in one of the three lists:
        # {
        #   'pod':          pod object from k8s list_pod_for_all_namespaces,
        #   'aisClientApi': openapi_client api for this daemon,
        #   'smap':         result of daemon smap query,
        #   'config':       result of daemon config query,
        #   'stats':        result of daemon stats query
        #   'snode':        result of daemon snode query
        # }
        #
        self.daemons = {
            'proxy':    [ { 'pod': pod  } for pod in proxy_pods ],
            'ne_proxy': [ { 'pod': pod  } for pod in ne_proxy_pods ],
            'target':   [ { 'pod': pod  } for pod in target_pods ]
        }

        #
        # Lookup Proxy ClusterIP Service
        # XXX Could/should add labels to services to shorten this
        #
        if not quiet:
            print("Querying AIS k8s services ...")
        svclist = self.v1api.list_service_for_all_namespaces(label_selector=self.proxySvcLabel).items

        self.service = {}
        self.service['proxyClusterIP'] = {}
        for svc in svclist:
            if svc.spec.type == 'ClusterIP' and svc.spec.cluster_ip != 'None':
                self.service['proxyClusterIP']['ip'] = svc.spec.cluster_ip
                self.service['proxyClusterIP']['port'] = svc.spec.ports[0].port
                break

    def refreshAisDaemonState(self, quiet=False):
        """Query AIS daemons for their state."""

        def createAisApiClients(daemonlist):
            """ Create openapi client API handles for a list of daemons (does not initiate connection yet). """

            for d in daemonlist:
                if d['pod'].status.pod_ip is None:
                    d['aisClientApi'] = None
                    continue

                config = openapi_client.Configuration()
                config.debug = False
                config.host = "http://%s:%s/v1" % (d['pod'].status.pod_ip, d['pod'].spec.containers[0].ports[0].container_port)
                d['aisClientApi'] = openapi_client.ApiClient(config)

        createAisApiClients(self.daemons['proxy'])
        createAisApiClients(self.daemons['ne_proxy'])
        createAisApiClients(self.daemons['target'])

        def aisDaemonQuery(daemonList, quiet=False):
            """Query the daemon api instance for smap, config, stats and snode info."""

            def query(d, key, what):
                """Thread function to grab daemon info."""
                d[key] = {}
                try:
                    d[key] = openapi_client.api.daemon_api.DaemonApi(d['aisClientApi']).get(what)
                except (MaxRetryError, NewConnectionError):
                    pass

            daemonQueries = (
                { 'key': 'smap',    'what': Ais.openapi_models.GetWhat.SMAP },
                { 'key': 'config',  'what': Ais.openapi_models.GetWhat.CONFIG },
                { 'key': 'stats',   'what': Ais.openapi_models.GetWhat.STATS },
                { 'key': 'snode',   'what': Ais.openapi_models.GetWhat.SNODE }
            )

            #
            # Make queries to all daemons in distinct threads per query - we don't want one
            # wayward daemon to hold us up.
            #
            started = 0
            for d in daemonList:
                if d['aisClientApi'] is None:
                    d['smap'] = {}
                    d['config'] = {}
                    d['stats'] = {}
                    d['snode'] = {}
                    d['_threadlist'] = [ ]
                    continue

                d['_threadlist'] = [ threading.Thread(target=query, name='%s:%s' % (d['pod'].metadata.name, q['key']), args = (d, q['key'], q['what'])) for q in daemonQueries ]
                for t in d['_threadlist']:
                    t.setDaemon(True)
                    t.start()
                    started += 1

            #
            # Join all threads on completion, limiting the time we'll wait
            #
            joined = 0
            t1 = datetime.datetime.now()
            while joined < started:
                for d in daemonList:
                    for t in d['_threadlist']:
                        rv = t.join(0.05)
                        if not t.isAlive():
                            joined += 1

                if (datetime.datetime.now() - t1).total_seconds() > 10:
                    break

            if joined != started:
                for d in daemonList:
                    stuck = []
                    for t in d['_threadlist']:
                        if t.isAlive():
                            stuck.append(t.getName())
                    if len(stuck) > 0:
                        print("  No response: %s" % ', '.join(stuck))


        if not quiet:
            print("Retrieving Smap/Config/Snode/Stats from each AIS daemon ...")
        
        aisDaemonQuery(self.daemons['proxy'])
        aisDaemonQuery(self.daemons['ne_proxy'])
        aisDaemonQuery(self.daemons['target'])
    
    def _aisNodeWalk(self, nodelist, cbfunc):
        """ Iterate over proxy, ne-proxy or target nodes with given callback."""

        for node in nodelist:
            if (cbfunc(node) != 0):
                break

    def aisProxyNodes(self):
        """Return list of nodes labelled for proxies."""
        return self.nodes_proxy

    def aisNeProxyNodes(self):
        """Return list of nodes labelled for ne proxies."""
        return self.nodes_neproxy

    def aisTargetNodes(self):
        """Return list of nodes labelled for targets."""
        return self.nodes_target

    def walkProxyNodes(self, cbfunc):
        """Walk proxy nodes with callback."""
        self._aisNodeWalk(self.nodes_proxy, cbfunc)

    def walkNeProxyNodes(self, cbfunc):
        """Walk ne-proxy nodes with callback."""
        self._aisNodeWalk(self.nodes_neproxy, cbfunc)

    def walkTargetNodes(self, cbfunc):
        """Walk target nodes with callback."""
        self._aisNodeWalk(self.nodes_target, cbfunc)

    def _aisPodWalk(self, daemonList, cbfunc):
        """ Iterate over proxy, ne-proxy or target pods with given callback."""

        for d in daemonList:
            if (cbfunc(d['pod'], smap=d['smap'], config=d['config'], stats=d['stats'], snode=d['snode'])) != 0:
                break

    def aisProxyPods(self):
        """Return list of electable proxy pods."""
        return [ d.pod for d in self.daemons.proxy ]

    def aisNeProxyPods(self):
        """Return list of non electable proxy pods."""
        return [ d.pod for d in self.daemons.ne_proxy ]

    def aisTargetPods(self):
        """Return list of target pods."""
        return [ d.pod for d in self.daemons.target ]

    def walkProxyPods(self, cbfunc):
        """Walk proxy pods with callback."""
        self._aisPodWalk(self.daemons['proxy'], cbfunc)

    def walkNeProxyPods(self, cbfunc):
        """Walk ne-proxy pods with callback."""
        self._aisPodWalk(self.daemons['ne_proxy'], cbfunc)

    def walkTargetPods(self, cbfunc):
        """Walk target pods with callback."""
        self._aisPodWalk(self.daemons['target'], cbfunc)

    def getInitialPrimaryNodeName(self):
        """Return node name labelled initial primary, or '-' if none."""
        if self.initialPrimaryNodeName is not None:
            return self.initialPrimaryNodeName
        else:
            return '-'

    def getProxyClusterSvc(self):
        """Return IP and port of clusterIP svc for proxies."""
        ip = self.service["proxyClusterIP"].get('ip', '-')
        port = self.service["proxyClusterIP"].get('port', '-')
        return ip, port

aisk8s = Ais()

def print_ais_topo(aishdl):
    nodename_ipp = aishdl.getInitialPrimaryNodeName()

    smap_info = {
        'latest_version': 0,
        'latest': None
    }

    ready_counts = {
        'proxy':    0,
        'ne_proxy': 0,
        'target':   0
    }

    print("Node labelling:\n\tProxy node(s): %d\n\tNon-electable proxy node(s): %d\n\tTarget node(s): %d" % 
        (len(aishdl.aisProxyNodes()), len(aishdl.aisNeProxyNodes()), len(aishdl.aisTargetNodes()) ))
    print("\tNode labelled as initial primary proxy: %s\n" % nodename_ipp)

    ip, port = aishdl.getProxyClusterSvc()
    print("\tProxy ClusterIP service: %s:%s\n" % (ip, port))

    cols = (
        { 'head': 'POD NAME',           'fmt': '%-25s' },
        { 'head': 'NODE',               'fmt': '%-11s' },
        { 'head': 'POD IP',             'fmt': '%-16s' },
        { 'head': 'RESTARTS',           'fmt': '%-9s' },
        { 'head': 'PODSTATE',           'fmt': '%-9s' },
        { 'head': 'LAST STATE CHANGE',  'fmt': '%-18s' },
        { 'head': 'READY',              'fmt': '%-5s' },
        { 'head': 'DAEMONID',           'fmt': '%-10s' },
        { 'head': 'SMAP',               'fmt': '%-4s' }
    )
    podlinefmt = '  ' + ' '.join([ col['fmt'] for col in cols ])
    podhdrline = podlinefmt % tuple([ col['head'] for col in cols ])

    def print_pod_info(pod, **kwargs):
        smap = kwargs['smap']
        config = kwargs['config']
        stats = kwargs['stats']
        snode = kwargs['snode']

        nodename = pod.spec.node_name
        if nodename is None:
            nodename = '(unscheduled)'
        component = pod.metadata.labels.get(u'component', None)
        if nodename == nodename_ipp and component == 'proxy':
            nodename += "*"
        else:
            nodename += ' '

        smap_version = smap.get(u'version', '-')
        if smap_version != '-' and int(smap_version) > smap_info['latest_version']:
            smap_info['latest_version'] = int(smap_version)
            smap_info['latest'] = smap

        now = datetime.datetime.now(pytz.utc)

        podstate = '-'
        since ='-'
        try:
            statemap = pod.status.container_statuses[0].state
            for attr, attrname in statemap.attribute_map.items():
                checkstate = getattr(statemap, attr, None)
                if checkstate is not None:
                    podstate = attrname
                    for csattr in checkstate.attribute_map.keys():
                        details = getattr(checkstate, csattr, None)
                        if details is not None and checkstate.swagger_types[csattr] == 'datetime':
                            delta = now - details
                            since = 't-' + str(delta).split('.')[0]
                            break
                    else:
                        since = '?'
                    break
        except TypeError:   # if statemap is not yet filled (early startup)
            pass

        if pod.status.container_statuses is not None:
            pod_restarts = pod.status.container_statuses[0].restart_count
            if pod.status.container_statuses[0].ready:
                pod_ready = "True"
                if component in ready_counts:
                    ready_counts[component] += 1
            else:
                pod_ready = "False"
        else:
            pod_restarts = '-'
            pod_ready = "False"

        daemonid = snode.get('daemon_id', '-')

        print(podlinefmt % (pod.metadata.name, nodename, pod.status.pod_ip, pod_restarts, podstate, since, pod_ready, daemonid, smap_version))
        return 0

    print(podhdrline)
    aishdl.walkProxyPods(print_pod_info)
    aishdl.walkNeProxyPods(print_pod_info)
    aishdl.walkTargetPods(print_pod_info)

    print("\nLatest observed smap version summary:")
    latest = smap_info['latest']
    if latest is not None:
        print("  %-25s: %d" % ("Version", smap_info['latest_version']))
        print("  %-25s: %s" % ("Current primary proxy", latest[u'proxy_si'][u'daemon_id']))
        print("  %-25s: %d (%d ready)" % ("Electable proxies: ", len(latest[u'pmap']) - len(latest[u'non_electable']), ready_counts['proxy']))
        print("  %-25s: %d (%d ready)" % ("Non-electable proxies: ", len(latest[u'non_electable']), ready_counts['ne_proxy']))
        print("  %-25s: %d (%d ready)" % ("Targets: ", len(latest[u'tmap']), ready_counts['target']))

print_ais_topo(aisk8s)

sys.exit(0)