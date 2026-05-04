import os, os.path
import contextlib
from . import verbose
from . import descr
from . import cmd
from . import revision_sql
from . import receivers
from . import pg_role_path
from . import scr_env
from . import init_sql

class InitCmdError(Exception):
    pass

def init_cmd(args_ctx, print_func, err_print_func):
    verb = verbose.make_verbose(print_func, err_print_func, args_ctx.verbose)

    verb.prepare_init()

    hosts_descr = descr.HostsDescr()

    if args_ctx.hosts is not None:
        hosts_path = os.path.realpath(args_ctx.hosts)

        hosts_descr.load(hosts_path)

    include_list = []
    include_ref_map = {}

    for include in args_ctx.include_list:
        include_list.append(os.path.realpath(include))

    for include_ref_name in args_ctx.include_ref_map:
        include_ref_map[include_ref_name] = \
                os.path.realpath(args_ctx.include_ref_map[include_ref_name])

    source_code_file_path = os.path.realpath(os.path.join(
        args_ctx.source_code,
        descr.ClusterDescr.file_name,
    ))
    source_code_include_list = include_list + [os.path.dirname(source_code_file_path)]
    source_code_cluster_descr = descr.ClusterDescr()

    source_code_cluster_descr.load(
            source_code_file_path, source_code_include_list, include_ref_map)

    if args_ctx.hosts is None:
        hosts_descr.load_pseudo(source_code_cluster_descr)

    cmd.apply_hosts_options(
            hosts_descr,
            args_ctx.host_name,
            args_ctx.define_map,
            args_ctx.conninfo,
            args_ctx.host_define_map)

    if args_ctx.exclusive and not cmd.is_single_host_run(hosts_descr):
        raise InitCmdError('unable to use --exclusive without a single-host run')

    rev_sql = revision_sql.RevisionSql(source_code_cluster_descr.application)

    verb.source_code_revision(
        source_code_cluster_descr.application,
        source_code_cluster_descr.revision,
        None,
    )

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
            source_code_role = cmd.schemas_role(source_code_cluster_descr, host_type)

            verb.begin_host(host_name)

            recv.begin_host(hosts_descr, host)

            recv.execute(host_name, pg_role_path.pg_role_path(source_code_role, None))

            if args_ctx.exclusive:
                verb.guard_exclusive(host_name, recv.look_fragment_i(host_name))

                recv.execute(host_name, rev_sql.guard_exclusive())

            verb.scr_env(host_name, recv.look_fragment_i(host_name))

            recv.execute(host_name, scr_env.scr_env(hosts_descr, host_name))

            verb.ensure_revision_structs(host_name, recv.look_fragment_i(host_name))

            recv.execute(host_name, rev_sql.ensure_revision_structs(host_type))

            for i, sql in enumerate(
                        init_sql.read_init_sql(source_code_cluster_descr, host_type),
                    ):
                if not i:
                    verb.execute_sql(
                            host_name, 'init_sql', recv.look_fragment_i(host_name))

                sql = pg_role_path.apply_pg_role_path(sql, source_code_role, None)

                verb.execute_sql(
                        host_name, 'init_sql', recv.look_fragment_i(host_name),
                        sql=sql)

                recv.execute(host_name, sql)

            recv.execute(host_name, pg_role_path.pg_role_path(source_code_role, None))

            verb.clean_scr_env(host_name, recv.look_fragment_i(host_name))

            recv.execute(host_name, scr_env.clean_scr_env())

            if args_ctx.exclusive:
                verb.clean_exclusive(host_name, recv.look_fragment_i(host_name))

                recv.execute(host_name, rev_sql.clean_exclusive())

            verb.finish_host(host_name)

            recv.finish_host(hosts_descr, host)

# vi:ts=4:sw=4:et
