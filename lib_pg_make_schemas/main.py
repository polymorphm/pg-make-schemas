import sys
import os
import argparse

class ArgsCtx:
    pass

def init_cmd(args_ctx, print_func, err_print_func):
    from . import init_cmd

    init_cmd.init_cmd(args_ctx, print_func, err_print_func)

def install_cmd(args_ctx, print_func, err_print_func):
    from . import install_cmd

    install_cmd.install_cmd(args_ctx, print_func, err_print_func)

def upgrade_cmd(args_ctx, print_func, err_print_func):
    from . import upgrade_cmd

    upgrade_cmd.upgrade_cmd(args_ctx, print_func, err_print_func)

def try_print(*args, **kwargs):
    kwargs.setdefault('flush', True)

    try:
        print(*args, **kwargs)
    except OSError:
        pass

def try_err_print(*args, **kwargs):
    kwargs.setdefault('file', sys.stderr)
    kwargs.setdefault('flush', True)

    try:
        print(*args, **kwargs)
    except OSError:
        pass

def main():
    parser = argparse.ArgumentParser(
        description='a utility for installing and upgrading database schemas '
                'from a versioned source-code repository.',
    )

    subparsers = parser.add_subparsers(
        dest='command',
    )

    init_parser = subparsers.add_parser(
        'init',
        help='run basic schema initialization, such as idempotent creation '
                'of extensions and roles',
        description='run basic schema initialization, such as idempotent '
                'creation of extensions and roles',
    )

    install_parser = subparsers.add_parser(
        'install',
        help='install schemas into a fresh database',
        description='install schemas into a fresh database',
    )

    upgrade_parser = subparsers.add_parser(
        'upgrade',
        help='upgrade schemas from a previous revision',
        description='upgrade schemas from a previous revision',
    )

    for sub_parser in (init_parser, install_parser, upgrade_parser):
        sub_parser.add_argument(
            '-v',
            '--verbose',
            action='count',
            help='show operations as they run. use twice to show more '
                    'details about executed SQL',
        )

        sub_parser.add_argument(
            '-e',
            '--execute',
            action='store_true',
            help='connect to databases and run SQL. this is the default when '
                    '``--output`` is not used. when used with ``--output``, '
                    'also writes PostgreSQL notice files next to SQL files',
        )

        sub_parser.add_argument(
            '-p',
            '--pretend',
            action='store_true',
            help='roll back database transactions instead of committing them. '
                    'this implies ``--execute``',
        )

        sub_parser.add_argument(
            '-o',
            '--output',
            help='prefix for generated SQL files. without ``--execute``, '
                    'this writes SQL files instead of connecting to databases. '
                    'with ``--execute``, this writes SQL files as an execution '
                    'record. warning: generated SQL may differ from live '
                    'database execution and should be reviewed carefully',
        )

        sub_parser.add_argument(
            '-i',
            '--include',
            action='append',
            help='add a directory to the allow-list for files referenced by '
                    'source-code or settings source-code trees. this option '
                    'can also define an include reference, '
                    'using name=value syntax. '
                    'you can use this option many times',
        )

    for sub_parser in (install_parser, upgrade_parser):
        sub_parser.add_argument(
            '-c',
            '--comment',
            action='store_true',
            help='run ``comment.sh`` to get the revision comment. warning: '
                    'use this only with trusted scripts. setting the '
                    '``PG_MAKE_SCHEMAS_COMMENT`` environment variable implies '
                    'this option',
        )

        sub_parser.add_argument(
            '--init',
            action='store_true',
            help='run basic initialization before install or upgrade. see the '
                    '``init`` command. the standalone ``init`` command may be '
                    'safer for some initialization tasks because it uses '
                    'different transaction management',
        )

    install_parser.add_argument(
        '--reinstall',
        action='store_true',
        help='drop schemas before creating new schemas, including variable '
                'schemas. warning: this deletes data',
    )

    install_parser.add_argument(
        '--reinstall-func',
        action='store_true',
        help='like ``--reinstall``, but do not touch variable schemas. this '
                'keeps data, but variable schemas may become incompatible '
                'with the recreated function schemas',
    )

    for sub_parser in (install_parser, upgrade_parser):
        sub_parser.add_argument(
            '--cascade',
            action='store_true',
            help='use DROP ... CASCADE when dropping schemas. warning: this '
                    'can be dangerous; review the possible consequences first'
        )

        sub_parser.add_argument(
            '-A',
            '--weak-acls',
            action='store_true',
            help='use weak ACL guarding for schemas instead of strict '
                    'guarding. this turns ``unexpected acl: ...`` errors into '
                    'notices'
        )

    upgrade_parser.add_argument(
        '--show-rev',
        action='store_true',
        help='only show revision information. use with ``--rev`` to check '
                'whether an upgrade path exists from a specific revision',
    )

    upgrade_parser.add_argument(
        '--change-rev',
        action='store_true',
        help='only change stored revision information. warning: this is '
                'dangerous; without ``--rev`` you can overwrite real revision '
                'information',
    )

    upgrade_parser.add_argument(
        '-r',
        '--rev',
        help='upgrade from this specific revision only. this can be useful '
                'when using pseudo-hosts with ``-`` or when '
                '``--show-rev``/``--change-rev`` is used',
    )

    upgrade_parser.add_argument(
        '--install',
        action='store_true',
        help='fall back to install for hosts that do not have a stored var '
                'revision',
    )

    for sub_parser in (init_parser, install_parser, upgrade_parser):
        sub_parser.add_argument(
            'hosts',
            help='path to the hosts file. if \'-\' is used, pseudo-hosts are '
                    'built from source-code schema types. this is useful when '
                    'the ``--output`` option is used',
        )

        arg_help_map = {
            init_parser: 'path to source code. only init files are used',
            install_parser: 'path to source code. migration files are not used',
            upgrade_parser: 'path to source code. migration files are used',
        }

        arg_help = arg_help_map[sub_parser]

        sub_parser.add_argument(
            'source_code',
            help=arg_help,
        )

        del arg_help
        del arg_help_map

    for sub_parser in (install_parser, upgrade_parser):
        if sub_parser == upgrade_parser:
            arg_help='path to settings source code. migration files are used'
        else:
            arg_help='path to settings source code. migration files are not used'

        sub_parser.add_argument(
            'settings_source_code',
            nargs='*',
            help=arg_help,
        )

        del arg_help

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()

        return

    args_ctx = ArgsCtx()

    args_ctx.command = args.command

    if args_ctx.command in ('init', 'install', 'upgrade'):
        args_ctx.verbose = args.verbose
        args_ctx.execute = args.execute
        args_ctx.pretend = args.pretend
        args_ctx.output = args.output
        args_ctx.hosts = args.hosts

        if args_ctx.pretend or args_ctx.output is None:
            args_ctx.execute = True

        if args_ctx.hosts == '-':
            args_ctx.hosts = None
    else:
        args_ctx.verbose = False
        args_ctx.execute = False
        args_ctx.pretend = False
        args_ctx.output = None
        args_ctx.hosts = None

    args_ctx.include_list = []
    args_ctx.include_ref_map = {}

    if args_ctx.command in ('init', 'install', 'upgrade') \
            and args.include is not None:
        for arg_inc in args.include:
            if '=' in arg_inc:
                arg_inc_ref_name, arg_inc_ref_val = arg_inc.split('=', 1)

                args_ctx.include_list.append(arg_inc_ref_val)
                args_ctx.include_ref_map[arg_inc_ref_name] = arg_inc_ref_val
            else:
                args_ctx.include_list.append(arg_inc)

    if args_ctx.command == 'install' and args.reinstall:
        args_ctx.reinstall = True
    else:
        args_ctx.reinstall = False

    if args_ctx.command == 'install' and args.reinstall_func:
        args_ctx.reinstall = True
        args_ctx.reinstall_func = True
    else:
        args_ctx.reinstall_func = False

    if args_ctx.command == 'upgrade':
        args_ctx.install = args.install
    else:
        args_ctx.install = False

    if args_ctx.command in ('install', 'upgrade'):
        args_ctx.comment = args.comment
        args_ctx.init = args.init
        args_ctx.cascade = args.cascade
        args_ctx.weak_guard_acls = args.weak_acls

        args_ctx.comment_path = os.environ.get('PG_MAKE_SCHEMAS_COMMENT')

        if args_ctx.comment_path is not None:
            args_ctx.comment = True
    else:
        args_ctx.comment = False
        args_ctx.init = False
        args_ctx.cascade = None
        args_ctx.weak_guard_acls = None
        args_ctx.comment_path = None

    if args_ctx.command == 'upgrade':
        args_ctx.show_rev = args.show_rev
        args_ctx.change_rev = args.change_rev
        args_ctx.rev = args.rev
    else:
        args_ctx.show_rev = False
        args_ctx.change_rev = False
        args_ctx.rev = None

    if args_ctx.command in ('init', 'install', 'upgrade'):
        args_ctx.source_code = args.source_code
    else:
        args_ctx.source_code = None

    if args_ctx.command in ('install', 'upgrade'):
        args_ctx.settings_source_code = args.settings_source_code
    else:
        args_ctx.settings_source_code = []

    cmd_func_map = {
        'init': init_cmd,
        'install': install_cmd,
        'upgrade': upgrade_cmd,
    }

    cmd_func = cmd_func_map[args_ctx.command]

    cmd_func(args_ctx, try_print, try_err_print)

# vi:ts=4:sw=4:et
