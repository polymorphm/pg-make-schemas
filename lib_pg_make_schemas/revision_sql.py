# -*- mode: python; coding: utf-8 -*-

from . import pg_literal

CREATE_REVISION_SCHEMA_SQL = '''\
create schema if not exists {q_revision_schema_ident};
revoke all on schema {q_revision_schema_ident} from public;\
'''

CREATE_REVISION_TABLE_SQL = '''\
create table if not exists {q_revision_schema_ident}.{q_revision_ident} (
application text primary key,
datetime timestamp with time zone not null,
revision text not null,
comment text,
schemas text[]
);\
'''

CREATE_ARCH_REVISION_TABLE_SQL = '''\
create table if not exists {q_revision_schema_ident}.{q_arch_revision_ident} (
id bigserial primary key,
arch_datetime timestamp with time zone not null,
application text not null,
datetime timestamp with time zone not null,
revision text not null,
comment text,
schemas text[]
);\
'''

FETCH_REVISION_SQL ='''\
select rev.revision, rev.comment
from {q_revision_schema_ident}.{q_revision_ident} rev
where rev.application = %(application)s
for update;\
'''

GUARD_REVISION_SQL ='''\
declare
_revision text;
begin
select rev.revision
into _revision
from {q_revision_schema_ident}.{q_revision_ident} rev
where rev.application = {q_application}
for update;
if _revision is distinct from {q_revision} then
raise 'unexpected revision: % %', quote_nullable (_revision), quote_nullable ({q_revision});
end if;
end\
'''

class RevisionSqlError(Exception):
    pass

class RevisionSqlUtils:
    @classmethod
    def application_ident(self, application):
        return application.replace('-', '_').lower()
    
    @classmethod
    def revision_schema_ident(cls, application_ident):
        return '{}_revision'.format(application_ident)
    
    @classmethod
    def revision_func_ident(cls, application_ident):
        return '{}_func_revision'.format(application_ident)
    
    @classmethod
    def revision_var_ident(cls, application_ident):
        return '{}_var_revision'.format(application_ident)
    
    @classmethod
    def arch_revision_func_ident(cls, application_ident):
        return 'arch_{}_func_revision'.format(application_ident)
    
    @classmethod
    def arch_revision_var_ident(cls, application_ident):
        return 'arch_{}_var_revision'.format(application_ident)

class RevisionSql:
    _create_revision_schema_sql=CREATE_REVISION_SCHEMA_SQL
    _create_revision_table_sql=CREATE_REVISION_TABLE_SQL
    _create_arch_revision_table_sql=CREATE_ARCH_REVISION_TABLE_SQL
    _fetch_revision_sql=FETCH_REVISION_SQL
    _guard_revision_sql=GUARD_REVISION_SQL
    _revision_sql_utils=RevisionSqlUtils
    
    def __init__(self, application):
        self._application = application
    
    def _pg_quote(self, value):
        return pg_literal.pg_quote(value)
    
    def _pg_ident_quote(self, ident):
        return pg_literal.pg_ident_quote(ident)
    
    def _pg_dollar_quote(self, tag, value):
        return pg_literal.pg_dollar_quote(tag, value)
    
    def _fetch_revision(self, recv, host_name, revision_schema_ident, revision_ident):
        con = recv.get_con(host_name)
        
        try:
            with con.cursor() as cur:
                cur.execute(
                    self._fetch_revision_sql.format(
                        q_revision_schema_ident=self._pg_ident_quote(
                            revision_schema_ident,
                        ).replace('%', '%%'),
                        q_revision_ident=self._pg_ident_quote(
                            revision_ident,
                        ).replace('%', '%%'),
                    ),
                    {
                        'application': self._application,
                    },
                )
                
                row = cur.fetchone()
                
                if row is None:
                    return None, None
                
                revision, comment = row
                
                return revision, comment
        except recv.con_error as e:
            raise RevisionSqlError('{!r}: {!r}: {}'.format(host_name, type(e), e)) from e
    
    def _guard_revision(self, revision_schema_ident, revision_ident, revision):
        guard_revision_body = self._guard_revision_sql.format(
            q_revision_schema_ident=self._pg_ident_quote(revision_schema_ident),
            q_revision_ident=self._pg_ident_quote(revision_ident),
            q_application=self._pg_quote(self._application),
            q_revision=self._pg_quote(revision),
        )
        
        return 'do {};'.format(self._pg_dollar_quote('do', guard_revision_body))
    
    def ensure_revision_structs(self):
        application_ident = self._revision_sql_utils.application_ident(self._application)
        q_revision_schema_ident = self._pg_ident_quote(
            self._revision_sql_utils.revision_schema_ident(application_ident),
        )
        
        create_list = [
            self._create_revision_schema_sql.format(
                q_revision_schema_ident=q_revision_schema_ident,
            ),
            self._create_revision_table_sql.format(
                q_revision_schema_ident=q_revision_schema_ident,
                q_revision_ident=self._pg_ident_quote(
                    self._revision_sql_utils.revision_func_ident(application_ident),
                ),
            ),
            self._create_revision_table_sql.format(
                q_revision_schema_ident=q_revision_schema_ident,
                q_revision_ident=self._pg_ident_quote(
                    self._revision_sql_utils.revision_var_ident(application_ident),
                ),
            ),
            self._create_arch_revision_table_sql.format(
                q_revision_schema_ident=q_revision_schema_ident,
                q_arch_revision_ident=self._pg_ident_quote(
                    self._revision_sql_utils.arch_revision_func_ident(application_ident),
                ),
            ),
            self._create_arch_revision_table_sql.format(
                q_revision_schema_ident=q_revision_schema_ident,
                q_arch_revision_ident=self._pg_ident_quote(
                    self._revision_sql_utils.arch_revision_var_ident(application_ident),
                ),
            ),
        ]
        
        return '\n\n'.join(create_list)
    
    def fetch_func_revision(self, recv, host_name):
        application_ident = self._revision_sql_utils.application_ident(self._application)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.revision_func_ident(application_ident)
        
        return self._fetch_revision(recv, host_name, revision_schema_ident, revision_ident)
    
    def fetch_var_revision(self, recv, host_name):
        application_ident = self._revision_sql_utils.application_ident(self._application)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.revision_var_ident(application_ident)
        
        return self._fetch_revision(recv, host_name, revision_schema_ident, revision_ident)
    
    def guard_func_revision(self, func_revision):
        application_ident = self._revision_sql_utils.application_ident(self._application)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.revision_func_ident(application_ident)
        
        return self._guard_revision(revision_schema_ident, revision_ident, func_revision)
    
    def guard_var_revision(self, var_revision):
        application_ident = self._revision_sql_utils.application_ident(self._application)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.revision_var_ident(application_ident)
        
        return self._guard_revision(revision_schema_ident, revision_ident, var_revision)
    
    
    # TODO      method: arch_func_revision()
    
    # TODO      method: arch_var_revision()
    
    # TODO      method: push_func_revision(func_revision, comment)
    
    # TODO      method: push_var_revision(var_revision, comment)
