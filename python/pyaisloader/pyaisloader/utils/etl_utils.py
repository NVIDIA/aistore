import aistore.sdk.etl_templates as etl_templates
from aistore.sdk.etl_const import ETL_COMM_HPUSH
from aistore.sdk.etl import Etl

from pyaisloader.utils.random_utils import generate_random_str

etls_created = []


def init_etl(client, spec_type):
    if spec_type == None:
        return None
    if not hasattr(etl_templates, spec_type):
        raise ValueError(f"ETL spec {spec_type} is not built-in")
    etl = client.etl(f"etl-{spec_type.lower()}-{generate_random_str()}")
    template = getattr(etl_templates, spec_type)
    spec = template.format(communication_type=ETL_COMM_HPUSH)
    etl.init_spec(template=spec)
    etl.spec_type = spec_type
    etls_created.append(etl)
    return etl


def cleanup_etls():
    for etl in etls_created:
        etl.stop()
        etl.delete()
