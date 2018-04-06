# -*- mode: python; coding: utf-8 -*-

import os, os.path
import contextlib
from . import descr
from . import receivers
from . import pg_search_path
from . import revision_sql
from . import scr_env
from . import init_sql

def init_cmd(args_ctx, print_func, err_print_func):
    hosts_descr = descr.HostsDescr()
    
    if args_ctx.hosts is not None:
        hosts_path = os.path.realpath(args_ctx.hosts)
        
        hosts_descr.load(hosts_path)
    
    include_list = []
    
    for include in args_ctx.include_list:
        include_list.append(os.path.realpath(include))
    
    source_code_file_path = os.path.realpath(os.path.join(
        args_ctx.source_code,
        descr.ClusterDescr.file_name,
    ))
    source_code_include_list = include_list + [os.path.dirname(source_code_file_path)]
    source_code_cluster_descr = descr.ClusterDescr()
    
    source_code_cluster_descr.load(source_code_file_path, source_code_include_list)
    
    if args_ctx.hosts is None:
        hosts_descr.load_pseudo(source_code_cluster_descr)
    
    rev_sql = revision_sql.RevisionSql(source_code_cluster_descr.application)
    
    with contextlib.closing(
                receivers.Receivers(
                    args_ctx.execute,
                    args_ctx.pretend,
                    args_ctx.output,
                ),
            ) as recv:
        recv.begin(hosts_descr)
        
        for host in hosts_descr.host_list:
            host_name = host['name']
            
            recv.execute(host_name, pg_search_path.pg_search_path(None))
            recv.execute(host_name, scr_env.scr_env(hosts_descr, host_name))
            recv.execute(host_name, rev_sql.ensure_revision_structs())
        
        for host in hosts_descr.host_list:
            host_name = host['name']
            host_type = host['type']
            
            for sql in init_sql.read_init_sql(source_code_cluster_descr, host_type):
                recv.execute(host_name, pg_search_path.pg_search_path(None))
                recv.execute(host_name, '{}\n\n;'.format(sql.rstrip()))
        
        for host in hosts_descr.host_list:
            host_name = host['name']
            
            recv.execute(host_name, pg_search_path.pg_search_path(None))
            recv.execute(host_name, scr_env.clean_scr_env())
        
        recv.done(hosts_descr)
