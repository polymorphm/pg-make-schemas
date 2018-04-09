# -*- mode: python; coding: utf-8 -*-

import os, os.path
import contextlib
from . import descr
from . import settings
from . import comment
from . import receivers
from . import install
from . import revision_sql
from . import pg_role_path
from . import scr_env
from . import install_sql
from . import settings_sql

def install_cmd(args_ctx, print_func, err_print_func):
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
    
    settings_cluster_descr_list = []
    
    if args_ctx.comment:
        comment_file_path = os.path.realpath(os.path.join(
            args_ctx.source_code,
            comment.COMMENT_FILE_NAME,
        ))
        
        com = comment.comment(comment_file_path)
    else:
        com = None
    
    for settings_source_code in args_ctx.settings_source_code:
        settings_file_path = os.path.realpath(os.path.join(
            settings_source_code,
            descr.ClusterDescr.file_name,
        ))
        settings_include_list = include_list + [os.path.dirname(settings_file_path)]
        settings_cluster_descr = descr.ClusterDescr()
        
        settings_cluster_descr.load(
            settings_file_path,
            settings_include_list,
            settingsMode=True,
        )
        
        settings.check_settings_compatibility(
            source_code_cluster_descr,
            settings_cluster_descr,
        )
        
        settings_cluster_descr_list.append(settings_cluster_descr)
    
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
            host_type = host['type']
            
            var_schemas = install.var_schemas(source_code_cluster_descr, host_type)
            func_schemas = install.func_schemas(source_code_cluster_descr, host_type)
            
            recv.execute(host_name, pg_role_path.pg_role_path('postgres', None))
            recv.execute(host_name, scr_env.scr_env(hosts_descr, host_name))
            recv.execute(host_name, rev_sql.ensure_revision_structs())
            
            if args_ctx.reinstall:
                if not args_ctx.reinstall_funcs:
                    recv.execute(host_name, rev_sql.arch_var_revision())
                    recv.execute(host_name, rev_sql.drop_var_schemas(var_schemas))
                
                recv.execute(host_name, rev_sql.arch_func_revision())
                recv.execute(host_name, rev_sql.drop_func_schemas(func_schemas))
            
            if not args_ctx.reinstall_funcs:
                recv.execute(host_name, rev_sql.guard_var_revision(None))
            
            recv.execute(host_name, rev_sql.guard_func_revision(None))
        
        if not args_ctx.reinstall_funcs:
            for host in hosts_descr.host_list:
                host_name = host['name']
                host_type = host['type']
                
                for schema_name, owner, grant_list, sql_iter in \
                        install_sql.read_var_install_sql(source_code_cluster_descr, host_type):
                    recv.execute(host_name, pg_role_path.pg_role_path('postgres', None))
                    recv.execute(
                        host_name, 
                        install_sql.create_schema(schema_name, owner, grant_list),
                    )
                    
                    for sql in sql_iter:
                        recv.execute(
                            host_name, '{}\n\n{}\n\n;'.format(
                                pg_role_path.pg_role_path(owner, schema_name),
                                sql.rstrip(),
                            ),
                        )
        
        for settings_cluster_descr in settings_cluster_descr_list:
            for host in hosts_descr.host_list:
                host_name = host['name']
                host_type = host['type']
                
                for sql in settings_sql.read_settings_sql(settings_cluster_descr, host_type):
                    recv.execute(
                        host_name, '{}\n\n{}\n\n;'.format(
                            pg_role_path.pg_role_path('postgres', None),
                            sql.rstrip(),
                        ),
                    )
        
        for host in hosts_descr.host_list:
            host_name = host['name']
            host_type = host['type']
            
            for schema_name, owner, grant_list, sql_iter in \
                    install_sql.read_func_install_sql(source_code_cluster_descr, host_type):
                recv.execute(host_name, pg_role_path.pg_role_path('postgres', None))
                recv.execute(
                    host_name, 
                    install_sql.create_schema(schema_name, owner, grant_list),
                )
                
                for sql in sql_iter:
                    recv.execute(
                        host_name, '{}\n\n{}\n\n;'.format(
                            pg_role_path.pg_role_path(owner, schema_name),
                            sql.rstrip(),
                        ),
                    )
        
        for host in hosts_descr.host_list:
            host_name = host['name']
            host_type = host['type']
            
            var_schemas = install.var_schemas(source_code_cluster_descr, host_type)
            func_schemas = install.func_schemas(source_code_cluster_descr, host_type)
            
            recv.execute(host_name, pg_role_path.pg_role_path('postgres', None))
            
            if not args_ctx.reinstall_funcs:
                for schema_name, owner, grant_list, sql_iter in \
                        install_sql.read_var_install_sql(source_code_cluster_descr, host_type):
                    recv.execute(
                        host_name,
                        install_sql.guard_acls(schema_name, owner, grant_list),
                    )
                
                recv.execute(
                    host_name,
                    rev_sql.push_var_revision(source_code_cluster_descr.revision, com, var_schemas),
                )
            
            for schema_name, owner, grant_list, sql_iter in \
                    install_sql.read_func_install_sql(source_code_cluster_descr, host_type):
                recv.execute(
                    host_name,
                    install_sql.guard_acls(schema_name, owner, grant_list),
                )
            
            recv.execute(
                host_name,
                rev_sql.push_func_revision(source_code_cluster_descr.revision, com, func_schemas),
            )
            
            recv.execute(host_name, scr_env.clean_scr_env())
        
        recv.done(hosts_descr)
