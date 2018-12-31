import logging
import argparse
import os
import re
import subprocess
import boto.ec2
import boto.vpc
from boto.s3.key import Key
import time
import json
import urllib2
import ConfigParser
from timeout import timeout

# Global vars assigned from the config in load_global_defaults
AWS_AK = None
AWS_SAK = None
security_group_name = 'DFC-SG1'
vpc_name = 'DFC-2'
subnet_name = 'DFC-2-Private'
gateway_name = 'DFC-2-GW'
route_table_name = 'DFC-2-RT1'
region_name = 'us-east-2'
cluster = None
logger = None

def setupLogger():
    global logger
    FORMAT = "%(asctime)s [ %(levelname)s ] %(message)s"
    logging.basicConfig(format=FORMAT)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

def load_global_defaults():
    global AWS_AK, AWS_SAK

    conf_path = '/home/ubuntu/aws.ini'
    c = ConfigParser.ConfigParser()
    c.optionxform = str
    c.read(conf_path)
    AWS_AK = c.get('aws_defaults', 'ak')
    AWS_SAK = c.get('aws_defaults', 'sak')
    
# Load global defaults
load_global_defaults()

def load_dfc_cluster(cluster, clients):
    dfc = {
        'targets' : None,
        'proxy'   : None,
        'clients' : None,
        'new_targets' : None
    }
    ec2_conn = ec2_connect_to_region()
    dfc['targets'] = ec2_conn.get_only_instances(filters={"tag:Name": cluster+'_Target*'})
    dfc['new_targets'] = ec2_conn.get_only_instances(filters={"tag:Name": cluster+'_NewTarget*'})
    dfc['proxy'] = ec2_conn.get_only_instances(filters={"tag:Name": cluster+'_Proxy*'})
    dfc['clients'] = ec2_conn.get_only_instances(filters={"tag:Name": cluster+'_Client*'})
    if len(dfc['clients']) > clients:
        dfc['clients'] = dfc['clients'][:clients]
        logger.info("Considering reduced number of clients {}".format(len(dfc['clients'])))
    
    cluster_inventory = os.path.join(os.path.dirname(__file__), 'inventory', 'cluster.ini')
    cluster_txt = os.path.join(os.path.dirname(__file__), 'inventory', 'cluster.txt')
    with open(cluster_inventory, 'w') as c, open(cluster_txt, 'w') as ct:
        for key in dfc:
            print key
            c.write('['+key+']\n')
            file = os.path.join(os.path.dirname(__file__), 'inventory', key+'.txt')
            client_count = 0
            with open(file, 'w') as f:
                for instance in dfc[key]:
                    ip = instance.private_ip_address
                    print ip
                    c.write(ip+'\n')
                    ct.write(ip+'\n')
                    f.write(ip+'\n')
    return dfc

def ec2_connect_to_region():
    try:
        conn = boto.ec2.connect_to_region(region_name, aws_access_key_id=AWS_AK,
                                          aws_secret_access_key=AWS_SAK)
    except Exception, e:
        logger.error("EC2 connection failed - {}".format(repr(e)))
        raise Exception("EC2 connection failed")
    return conn

def vpc_connect_to_region(region_name):
    try:
        conn =  boto.vpc.connect_to_region(region_name)
    except:
        conn = boto.vpc.connect_to_region(region_name,aws_access_key_id=AWS_AK,
                                          aws_secret_access_key=AWS_SAK)
    return conn

def create_vpc(vpc_conn, ec2_conn):
    vpc_list = vpc_conn.get_all_vpcs(filters={'tag:Name':vpc_name})
    if(len(vpc_list) > 0):
        logger.info(vpc_list[0].id)
        return vpc_list[0].id
    else:
        logger.info('creating vpc:'+vpc_name);
        vpc = vpc_conn.create_vpc('10.0.0.0/16',instance_tenancy='default')
        logger.info (vpc.id)
        ec2_conn.create_tags([vpc.id],{'Name':vpc_name})
        return vpc.id
    
def create_subnet(vpc_conn,ec2_conn,vpc_id):
    subnet_list = vpc_conn.get_all_subnets(filters={'tag:Name':subnet_name})
    if(len(subnet_list) > 0):
        logger.info (subnet_list[0].id)
        return subnet_list[0].id
    else:
        logger.info('creating subnet:'+subnet_name);
        subnet = vpc_conn.create_subnet(vpc_id,'10.0.0.0/16')
        logger.info (subnet.id)
        ec2_conn.create_tags([subnet.id],{'Name':subnet_name})
        return subnet.id
        
def create_security_group(ec2_conn,vpc_id):
    sg_list = ec2_conn.get_all_security_groups(filters={'group_name':security_group_name}) 
    if(len(sg_list) > 0):
        return sg_list[0].id
    else:
        logger.info('creating security group:'+security_group_name);
        sg = ec2_conn.create_security_group(security_group_name,'diagnolab test server sg',vpc_id=vpc_id)
        sg.authorize(ip_protocol="tcp", from_port=3389, to_port=3389, cidr_ip="202.135.238.200/32",src_group=None)
        sg.authorize(ip_protocol="tcp", from_port=1, to_port=3388, cidr_ip="0.0.0.0/0",src_group=None)
        sg.authorize(ip_protocol="tcp", from_port=3390, to_port=65535, cidr_ip="0.0.0.0/0",src_group=None)
        sg.authorize(ip_protocol="udp", from_port=1, to_port=65535, cidr_ip="0.0.0.0/0",src_group=None)
        sg.authorize(ip_protocol="icmp", from_port=-1, to_port=-1, cidr_ip="0.0.0.0/0",src_group=None)
        return sg.id
    
def create_internet_gateway(vpc_conn,ec2_conn,vpc_id):
    gateway_list = vpc_conn.get_all_internet_gateways(filters={'tag:Name':gateway_name})
    if(len(gateway_list) > 0):
        logger.info (gateway_list[0].id)
        return gateway_list[0].id
    else:
        logger.info('creating internet gateway:'+gateway_name);
        gateway = vpc_conn.create_internet_gateway();
        logger.info (gateway.id)
        ec2_conn.create_tags([gateway.id],{'Name':gateway_name})
        vpc_conn.attach_internet_gateway(gateway.id,vpc_id)
        return gateway.id

def create_route_table(vpc_conn,ec2_conn,vpc_id,gateway_id,sunbet_id):
    route_table_list = vpc_conn.get_all_route_tables(filters={'tag:Name':route_table_name})
    if(len(route_table_list) > 0):
        logger.info (route_table_list[0].id)
        return route_table_list[0].id
    else:
        logger.info('creating route table:'+route_table_name);
        route_table = vpc_conn.create_route_table(vpc_id)
        logger.info (route_table.id)
        ec2_conn.create_tags([route_table.id],{'Name':route_table_name})
        vpc_conn.create_route(route_table.id,destination_cidr_block='0.0.0.0/0',gateway_id=gateway_id)
        vpc_conn.associate_route_table(route_table.id,sunbet_id)
    
def launch_instance(region_name,ami_id):
    instance_ip = None;
    vpc_conn = vpc_connect_to_region(region_name)
    ec2_conn = ec2_connect_to_region(region_name)
    vpc_id = create_vpc(vpc_conn,ec2_conn)
    gateway_id = create_internet_gateway(vpc_conn,ec2_conn,vpc_id)
    sg_id = create_security_group(ec2_conn,vpc_id)
    sunbet_id = create_subnet(vpc_conn,ec2_conn,vpc_id)
    create_route_table(vpc_conn,ec2_conn,vpc_id,gateway_id,sunbet_id)
    interface = boto.ec2.networkinterface.NetworkInterfaceSpecification(subnet_id=sunbet_id,
                                                                    groups=[sg_id],
                                                                    associate_public_ip_address=True)
    interfaces = boto.ec2.networkinterface.NetworkInterfaceCollection(interface)
    if(ami_id != None):
        reservation = ec2_conn.run_instances(ami_id,
                                             key_name=None,
                                             #security_groups=[security_group_name],
                                             #subnet_id=sunbet_id,
                                             instance_type ='c3.large',
                                             network_interfaces=interfaces,
                                             instance_initiated_shutdown_behavior='terminate')
        for instance in reservation.instances:
            logger.info (instance.id+' state='+instance.state)
        instance = reservation.instances[0]
        instance.update()
        while(instance.state != 'running'):
            time.sleep(5)
            instance.update()
            logger.info (instance.id+' state='+instance.state)
        instance_ip = instance.ip_address
        instance.add_tag("Name","Diagnolab Test server "+version)
    return instance_ip

def check_instance_health(server_address):
    try:
        return urllib2.urlopen("http://"+server_address+":8080/v1/health/").getcode()
    except:
        return 503

@timeout(600)    
def start_stop_instance(instances, state):
    for instance in instances:
        if (instance.state != state):
            logger.info("Instance {0} state is {1}, trying to change it to {2}".format(instance.id, instance.state, state))
            if state == 'running':
                instance.start()
            if state == 'stopped':
                instance.stop()
            instance.update()
        else:
            logger.info("Instance {0} state is matching the desired state {1}".format(instance.id, instance.state))

    for instance in instances:
        instance.update()
        logger.info("Instance {0} state changed to {1}".format(instance.id, instance.state))
        while(instance.state != state):
            time.sleep(5)
            instance.update()
            logger.info("Instance {0} state changed to {1}".format(instance.id, instance.state))

def terminate_instance(region_name,ip_address):
    ec2_conn = ec2_connect_to_region(region_name)
    instances = ec2_conn.get_only_instances(filters={"ip-address": ip_address})
    if(len(instances)> 0 ):
        instances[0].terminate()
    else:
        logger.info('failed to terminate instance with ip '+str(ip_address))

def start_dfc_cluster(dfc):
    for key in dfc:
        logger.info("Booting dfc {}".format(key))
        start_stop_instance(dfc[key], 'running')
    #Additional sleep to make sure every instance is up for SSH connection
    time.sleep(15)

def stop_dfc_cluster(dfc):
    for key in dfc:
        logger.info("Shutting down dfc {}".format(key))
        start_stop_instance(dfc[key], 'stopped')

def update_dfc_cluster(dfc):
    subprocess.call('./updatedfc.sh')

def cleanup_dfc_cluster(dfc):
    subprocess.call('./cleandfc.sh')
    
if __name__ == '__main__':

    setupLogger()

    parser = argparse.ArgumentParser(description="", add_help=False)
    parser.add_argument("--help", action="help")
    parser.add_argument("--cluster", dest='cluster', required=True, help="Name of the cluster to operate on")
    parser.add_argument("--command", dest="command", required=True, help="Supported commands - create, terminate, restart, shutdown, update, cleanup")
    parser.add_argument("--clients", dest="clients", required=False, help="Number of clients to use, default 4", default=4)

    args = parser.parse_args()

    cluster = args.cluster
    clients = int(args.clients)
    dfc = load_dfc_cluster(cluster, clients)

    if args.command == 'restart':
        start_dfc_cluster(dfc)
    elif args.command == 'shutdown':
        stop_dfc_cluster(dfc)
    elif args.command == 'update':
        update_dfc_cluster(dfc)
    elif args.command == 'cleanup':
        cleanup_dfc_cluster(dfc)

