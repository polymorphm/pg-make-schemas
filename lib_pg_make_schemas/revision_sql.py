# -*- mode: python; coding: utf-8 -*-

from . import pg_literal

CREATE_REVISION_STRUCTS_SQL = '''\
create schema if not exists {q_revision_schema_ident};
alter schema {q_revision_schema_ident} owner to postgres;
revoke all on schema {q_revision_schema_ident} from public;

create table if not exists {q_revision_schema_ident}.{q_revision_func_ident} (
application text primary key,
datetime timestamp with time zone not null,
revision text not null,
comment text,
schemas text[]
);
alter table {q_revision_schema_ident}.{q_revision_func_ident} owner to postgres;

create table if not exists {q_revision_schema_ident}.{q_revision_var_ident} (
application text primary key,
datetime timestamp with time zone not null,
revision text not null,
comment text,
schemas text[]
);
alter table {q_revision_schema_ident}.{q_revision_var_ident} owner to postgres;

create table if not exists {q_revision_schema_ident}.{q_arch_revision_func_ident} (
id bigserial primary key,
arch_datetime timestamp with time zone not null,
application text not null,
datetime timestamp with time zone not null,
revision text not null,
comment text,
schemas text[]
);
alter table {q_revision_schema_ident}.{q_arch_revision_func_ident} owner to postgres;

create table if not exists {q_revision_schema_ident}.{q_arch_revision_var_ident} (
id bigserial primary key,
arch_datetime timestamp with time zone not null,
application text not null,
datetime timestamp with time zone not null,
revision text not null,
comment text,
schemas text[]
);
alter table {q_revision_schema_ident}.{q_arch_revision_var_ident} owner to postgres;\
'''

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
    _create_revision_structs_sql=CREATE_REVISION_STRUCTS_SQL
    _revision_sql_utils=RevisionSqlUtils
    
    def __init__(self, application):
        self._application = application
    
    def _pg_ident_quote_func(self, ident):
        return pg_literal.pg_ident_quote(ident)
    
    def ensure_revision_structs(self):
        application_ident = self._revision_sql_utils.application_ident(self._application)
        
        return self._create_revision_structs_sql.format(
            q_revision_schema_ident=self._pg_ident_quote_func(
                self._revision_sql_utils.revision_schema_ident(application_ident)
            ),
            q_revision_func_ident=self._pg_ident_quote_func(
                self._revision_sql_utils.revision_func_ident(application_ident)
            ),
            q_revision_var_ident=self._pg_ident_quote_func(
                self._revision_sql_utils.revision_var_ident(application_ident)
            ),
            q_arch_revision_func_ident=self._pg_ident_quote_func(
                self._revision_sql_utils.arch_revision_func_ident(application_ident)
            ),
            q_arch_revision_var_ident=self._pg_ident_quote_func(
                self._revision_sql_utils.arch_revision_var_ident(application_ident)
            ),
        )
    
    # TODO      method: fetch_func_revision(recv, host_name) ### select .. for update
    
    # TODO      method: fetch_var_revision(recv, host_name) ### select .. for update
    
    # TODO      method: guard_func_revision(func_revision) ### select .. for update
    
    # TODO      method: guard_var_revision(var_revision) ### select .. for update
    
    # TODO      method: arch_func_revision()
    
    # TODO      method: arch_var_revision()
    
    # TODO      method: push_func_revision(func_revision, comment)
    
    # TODO      method: push_var_revision(var_revision, comment)
