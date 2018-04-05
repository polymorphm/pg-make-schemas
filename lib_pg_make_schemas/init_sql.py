# -*- mode: python; coding: utf-8 -*-

def read_init_sql(cluster_descr, host_type):
    for schemas in cluster_descr.schemas_list:
        if schemas.schemas_type != host_type:
            continue
        
        init = schemas.init
        
        if init is None:
            continue
        
        yield from init.read_sql()
