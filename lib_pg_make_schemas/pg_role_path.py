# -*- mode: python; coding: utf-8 -*-

from . import pg_literal

def pg_role_path(
            role,
            schema_name,
            pg_ident_quote_func=pg_literal.pg_ident_quote,
        ):
    set_list = [
        'set local role to {};'.format(
            pg_ident_quote_func(role),
        )
    ]
    
    if schema_name is not None:
        set_list.append(
            'set local search_path to {}, public;'.format(
                pg_ident_quote_func(schema_name),
            ),
        )
    else:
        set_list.append('set local search_path to public;')
    
    return '\n'.join(set_list)
