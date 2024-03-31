from . import pg_literal

def pg_role_path(
            role,
            schema_name,
            pg_ident_quote_func=pg_literal.pg_ident_quote,
        ):
    if role is not None:
        set_list = [
            'set local role to {};'.format(
                pg_ident_quote_func(role),
            ),
        ]
    else:
        set_list = ['set local role to postgres;']

    if schema_name is not None:
        set_list.append(
            'set local search_path to {};'.format(
                pg_ident_quote_func(schema_name),
            ),
        )
    else:
        set_list.append('set local search_path to \'\';')

    set_list.append('set local check_function_bodies to off;')

    return '\n'.join(set_list)

def apply_pg_role_path(
            sql,
            role,
            schema_name,
            pg_role_path_func=pg_role_path,
            pg_ident_quote_func=pg_literal.pg_ident_quote,
        ):
    return '{}\n\n{}\n\n;'.format(
        pg_role_path_func(role, schema_name, pg_ident_quote_func=pg_ident_quote_func),
        sql.rstrip(),
    )

# vi:ts=4:sw=4:et
