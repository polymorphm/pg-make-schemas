# -*- mode: python; coding: utf-8 -*-

from . import pg_literal

def pg_search_path(
            schema_name,
            pg_ident_quote_func=pg_literal.pg_ident_quote,
        ):
    if schema_name is None:
        return 'set local search_path to public;'
    
    return 'set local search_path to {}, public;'.format(
        pg_ident_quote_func(schema_name),
    )
