# -*- mode: python; coding: utf-8 -*-

def read_safeguard_sql(cluster_descr, host_type):
    for schemas_descr in cluster_descr.schemas_list:
        if schemas_descr.schemas_type != host_type:
            continue
        
        safeguard_descr = schemas_descr.safeguard
        
        if safeguard_descr is None:
            continue
        
        yield from safeguard_descr.read_sql()
