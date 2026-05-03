from . import pg_literal

DEFAULT_ROLE = 'postgres'

class PgRolePath:
    _default_role = DEFAULT_ROLE
    _pg_ident_quote = staticmethod(pg_literal.pg_ident_quote)

    def pg_role_path(self, role, schema_name):
        if role is not None:
            effective_role = role
        else:
            effective_role = self._default_role

        set_list = [
            'set local role to {};'.format(
                self._pg_ident_quote(effective_role),
            ),
        ]

        if schema_name is not None:
            set_list.append(
                'set local search_path to {};'.format(
                    self._pg_ident_quote(schema_name),
                ),
            )
        else:
            set_list.append('set local search_path to \'\';')

        set_list.append('set local check_function_bodies to off;')

        return '\n'.join(set_list)

    def apply_pg_role_path(self, sql, role, schema_name):
        if isinstance(sql, tuple):
            sql_list_or_str, sql_info = sql
            if isinstance(sql_list_or_str, list):
                sql_str_list = sql_list_or_str
            elif isinstance(sql_list_or_str, str):
                sql_str_list = [sql_list_or_str]
            else:
                raise TypeError
        elif isinstance(sql, str):
            sql_str, sql_info = sql, {}
            sql_str_list = [sql_str]
        else:
            raise TypeError

        if not sql_str_list:
            return sql_str_list, sql_info

        if role is not None:
            effective_role = role
        else:
            effective_role = self._default_role

        new_sql_str_list = [
            '{}\n\n'.format(
                self.pg_role_path(role, schema_name),
            )
        ] + sql_str_list[:-1] + ['{}\n\n;'.format(sql_str_list[-1].rstrip())]

        new_sql_info = sql_info.copy()
        new_sql_info.update({
            'pg_role': effective_role,
            'pg_search_path': schema_name,
        })

        return new_sql_str_list, new_sql_info

_pg_role_path = PgRolePath()

def pg_role_path(role, schema_name, pg_role_path_obj=None):
    if pg_role_path_obj is None:
        pg_role_path_obj = _pg_role_path

    return pg_role_path_obj.pg_role_path(role, schema_name)

def apply_pg_role_path(sql, role, schema_name, pg_role_path_obj=None):
    if pg_role_path_obj is None:
        pg_role_path_obj = _pg_role_path

    return pg_role_path_obj.apply_pg_role_path(sql, role, schema_name)

# vi:ts=4:sw=4:et
