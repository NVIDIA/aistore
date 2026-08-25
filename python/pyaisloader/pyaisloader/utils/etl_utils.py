from pyaisloader.utils.random_utils import generate_random_str

etls_created = []


def init_etl(client, spec_type):
    if spec_type == None:
        return None
    etl = client.etl(f"etl-{spec_type.lower()}-{generate_random_str()}")
    etl.init(image=f"aistorage/transformer_{spec_type.lower()}:latest")
    etl.spec_type = spec_type
    etls_created.append(etl)
    return etl


def cleanup_etls():
    for etl in etls_created:
        etl.stop()
        etl.delete()
