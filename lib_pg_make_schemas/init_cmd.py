# -*- mode: python; coding: utf-8 -*-

import os, os.path
import contextlib
from . import verbose
from . import descr
from . import revision_sql
from . import receivers
from . import pg_role_path
from . import scr_env
from . import init_sql

def init_cmd(args_ctx, print_func, err_print_func):
    verb = verbose.make_verbose(print_func, err_print_func, args_ctx.verbose)
    
    verb.prepare_init()
    
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
    
    verb.source_code_revision(source_code_cluster_descr.revision, None)
    
    with contextlib.closing(
                receivers.Receivers(
                    args_ctx.execute,
                    args_ctx.pretend,
                    args_ctx.output,
                ),
            ) as recv:
        for host in hosts_descr.host_list:
            host_name = host['name']
            host_type = host['type']
            
            verb.begin_host(host_name)
            recv.begin_host(hosts_descr, host)
            
            recv.execute(host_name, pg_role_path.pg_role_path('postgres', None))
            recv.execute(host_name, scr_env.scr_env(hosts_descr, host_name))
            recv.execute(host_name, rev_sql.ensure_revision_structs())
            
            for sql in init_sql.read_init_sql(source_code_cluster_descr, host_type):
                recv.execute(
                    host_name, '{}\n\n{}\n\n;'.format(
                        pg_role_path.pg_role_path('postgres', None),
                        sql.rstrip(),
                    ),
                )
            
            recv.execute(host_name, pg_role_path.pg_role_path('postgres', None))
            recv.execute(host_name, scr_env.clean_scr_env())
            
            verb.finish_host(host_name)
            recv.finish_host(hosts_descr, host)
