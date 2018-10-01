# -*- mode: python; coding: utf-8 -*-

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
        description='an utility for installing and upgrading database schemas '
                'from a revisioned source code repository.',
    )
    
    subparsers = parser.add_subparsers(
        dest='command',
    )
    
    init_parser = subparsers.add_parser(
        'init',
        help='do some basic initialization of schemas, e.g., '
                'idempotent creation of extensions and roles',
        description='do some basic initialization of schemas, e.g., '
                'idempotent creation of extensions and roles',
    )
    
    install_parser = subparsers.add_parser(
        'install',
        help='do fresh installing schemas',
        description='do fresh installing schemas',
    )
    
    upgrade_parser = subparsers.add_parser(
        'upgrade',
        help='do upgrading schemas from one of previous revisions',
        description='upgrading schemas from one of previous revisions',
    )
    
    for sub_parser in (init_parser, install_parser, upgrade_parser):
        sub_parser.add_argument(
            '-v',
            '--verbose',
            action='store_true',
            help='be verbose. there will be every operation shown',
        )
        
        sub_parser.add_argument(
            '-e',
            '--execute',
            action='store_true',
            help='do database interactions. '
                    'it is default when the ``--output`` option is not used',
        )
        
        sub_parser.add_argument(
            '-p',
            '--pretend',
            action='store_true',
            help='use rollbacks instead of commits after doing database interactions. '
                    'this implies ``--execute`` option',
        )
        
        sub_parser.add_argument(
            '-o',
            '--output',
            help='prefix to output SQL files. this makes output SQL files '
                    'instead of doing database interactions (besides '
                    'doing database interactions when the ``--execute`` option is used). '
                    'warning(!) the result may be different from the result of database interactions. '
                    'the output code is less smart and it can be more dangerous',
        )
        
        sub_parser.add_argument(
            '-i',
            '--include',
            action='append',
            help='add this path to allowed list of directories which can be '
                    'refered from source code files or settings source code files. '
                    'this option can also be used to define include-reference, '
                    'using name=value syntax. '
                    'you can use this option many times',
        )
    
    for sub_parser in (install_parser, upgrade_parser):
        sub_parser.add_argument(
            '-c',
            '--comment',
            action='store_true',
            help='use ``comment.sh`` shell script for getting revision comment. '
                    'warning(!) before using this option make sure the shell script '
                    'is from a trusted origin. using environment variable '
                    '``PG_MAKE_SCHEMAS_COMMENT`` implies this option',
        )
        
        sub_parser.add_argument(
            '--init',
            action='store_true',
            help='do some basic initialization. see ``init`` command. '
                    'pay attention that ``--init`` option might work worse than '
                    'standalone ``init`` command works due to different '
                    'transaction management',
        )
    
    install_parser.add_argument(
        '--reinstall',
        action='store_true',
        help='do dropping schemas before creating new schemas '
                'including variable schemas. '
                'warning(!) your data will be deleted',
    )
    
    install_parser.add_argument(
        '--reinstall-func',
        action='store_true',
        help='it is like ``--reinstall`` option, but doesn\'t touch variable schemas. '
                'so your data will be safe, but variable schemas '
                'might become incompatible with created function schemas',
    )
    
    for sub_parser in (install_parser, upgrade_parser):
        sub_parser.add_argument(
            '--cascade',
            action='store_true',
            help='use drop with cascade when dropping schemas. '
                    'warning(!) using this option can be dangerous, make sure '
                    'you understand what you do and understand possible consequences'
        )
    
    upgrade_parser.add_argument(
        '--show-rev',
        action='store_true',
        help='do nothing except showing revision information. '
                'you can use ``--rev`` option for checking ability '
                'upgrading from a specific revision',
    )
    
    upgrade_parser.add_argument(
        '--change-rev',
        action='store_true',
        help='do nothing except changing revision information. '
                'warning(!) it is dangerous feature, you can mistakenly lose real revision information '
                'when the ``--rev`` option is not used',
    )
    
    upgrade_parser.add_argument(
        '-r',
        '--rev',
        help='do upgrading from this specific revision only. '
                'that may be useful when the hosts file is empty '
                'or when ``--show-rev``/``--change-rev`` option is used',
    )
    
    for sub_parser in (init_parser, install_parser, upgrade_parser):
        sub_parser.add_argument(
            'hosts',
            help='path to the hosts file. if \'-\' is used, it is '
                    'considered as an empty hosts file. that may be useful '
                    'when the ``--output`` option is used',
        )
        
        arg_help_map = {
            init_parser: 'path to source code. will be used init files only',
            install_parser: 'path to source code. won\'t be used migration files',
            upgrade_parser: 'path to source code. will be used migration files',
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
            arg_help='path to settings source code. will be used migration files'
        else:
            arg_help='path to settings source code. won\'t be used migration files'
        
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
    
    if args_ctx.command in ('install', 'upgrade'):
        args_ctx.comment = args.comment
        args_ctx.init = args.init
        args_ctx.cascade = args.cascade
        
        args_ctx.comment_path = os.environ.get('PG_MAKE_SCHEMAS_COMMENT')
        
        if args_ctx.comment_path is not None:
            args_ctx.comment = True
    else:
        args_ctx.comment = False
        args_ctx.init = False
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
