# -*- mode: python; coding: utf-8 -*-

import contextlib
import os, os.path
import yaml

class LoadUtils:
    @classmethod
    def check_and_open_for_r(cls, file_path, include_list):
        for include in include_list:
            if os.path.commonpath((file_path, include)) == include:
                break
        else:
            raise ValueError(
                '{!r}: this file is not in any directory which is included to allowed list'.format(
                    file_path,
                ),
            )
        
        fileno = None
        fd = None
        
        try:
            fileno = os.open(file_path, os.O_NOFOLLOW)
            
            if os.path.isdir('/proc'):
                opened_file_path = os.readlink('/proc/self/fd/{}'.format(fileno))
                
                if file_path != opened_file_path:
                    raise OSError(
                        '{!r}, {!r}: the opened file has unexpectedly changed to another'.format(
                            file_path,
                            opened_file_path,
                        ),
                    )
            
            # XXX   portability issue:
            #       we have no a full safe implementation here if there is no ``/proc``
            
            fd = os.fdopen(fileno, encoding='utf-8')
        finally:
            if fileno is not None and fd is None:
                os.close(fileno)
        
        return fd
    
    @classmethod
    def check_include_elem(cls, include_elem, first_elem, last_elem):
        if include_elem is not None and not isinstance(include_elem, (list, str)):
            raise ValueError('not isinstance(include_elem, (list, str))')
        
        if first_elem is not None and not isinstance(first_elem, (list, str)):
            raise ValueError('not isinstance(first_elem, (list, str))')
        
        if last_elem is not None and not isinstance(last_elem, (list, str)):
            raise ValueError('not isinstance(last_elem, (list, str))')
    
    @classmethod
    def load_file_path_list(
                cls,
                file_dir,
                include_elem, first_elem, last_elem,
                filt_func,
            ):
        path_list = []
        file_path_list = []
        file_path_set = set()
        first_file_path_list = []
        last_file_path_list = []
        
        if isinstance(include_elem, str):
            include_elem = [include_elem]
        
        if isinstance(first_elem, str):
            first_elem = [first_elem]
        
        if isinstance(last_elem, str):
            last_elem = [last_elem]
        
        if include_elem is not None:
            for include_item_elem in include_elem:
                if not isinstance(include_item_elem, str):
                    raise ValueError('not isinstance(include_item_elem, str)')
                
                path = os.path.realpath(os.path.join(
                    file_dir,
                    include_item_elem,
                ))
                
                path_list.append(path)
        
        path_list.append(file_dir)
        
        for path in path_list:
            for f in sorted(d.name for d in os.scandir(path)):
                file_path = os.path.realpath(os.path.join(path, f))
                
                if not filt_func(file_path):
                    continue
                
                if file_path in file_path_set:
                    raise ValueError('{!r}: this file is duplicated'.format(file_path))
                
                file_path_set.add(file_path)
                file_path_list.append(file_path)
        
        for ordered_elem, ordered_file_path_list in [
                    (first_elem, first_file_path_list),
                    (last_elem, last_file_path_list),
                ]:
            if ordered_elem is not None:
                for ordered_item_elem in ordered_elem:
                    if not isinstance(ordered_item_elem, str):
                        raise ValueError('not isinstance(ordered_item_elem, str)')
                    
                    file_is_used = False
                    
                    for path in path_list:
                        file_path = os.path.realpath(os.path.join(
                            path,
                            ordered_item_elem,
                        ))
                        
                        if file_path not in file_path_list:
                            continue
                        
                        file_is_used = True
                        file_path_list.remove(file_path)
                        ordered_file_path_list.append(file_path)
                    
                    if not file_is_used:
                        raise ValueError('{!r}: this file is not used'.format(ordered_item_elem))
        
        return file_path_list, first_file_path_list, last_file_path_list
    
    @classmethod
    def read_content(
                cls,
                file_path_list, first_file_path_list, last_file_path_list,
                inline,
                include_list,
            ):
        if first_file_path_list is not None:
            for file_path in first_file_path_list:
                with cls.check_and_open_for_r(file_path, include_list) as fd:
                    yield fd.read()
        
        if file_path_list is not None:
            for file_path in file_path_list:
                with cls.check_and_open_for_r(file_path, include_list) as fd:
                    yield fd.read()
        
        if inline is not None:
            yield inline
        
        if last_file_path_list is not None:
            for file_path in last_file_path_list:
                with cls.check_and_open_for_r(file_path, include_list) as fd:
                    yield fd.read()

class HostsDescr:
    def load(self, hosts_file_path):
        with open(hosts_file_path, encoding='utf-8') as fd:
            doc = yaml.safe_load(fd)
        
        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')
        
        hosts_elem = doc['hosts']
        
        if not isinstance(hosts_elem, list):
            raise ValueError('not isinstance(hosts_elem, list)')
        
        host_list = []
        
        for host_elem in hosts_elem:
            if not isinstance(host_elem, dict):
                raise ValueError('not isinstance(host_elem, dict)')
            
            host_name = host_elem['name']
            host_type = host_elem.get('type')
            host_conninfo = host_elem.get('conninfo')
            host_params = host_elem.get('params')
            
            if not isinstance(host_name, str):
                raise ValueError('not isinstance(host_name, str)')
            
            if host_type is None:
                host_type = host_name
            elif not isinstance(host_type, str):
                raise ValueError('not isinstance(host_type, str)')
            
            if host_conninfo is not None and not isinstance(host_conninfo, str):
                raise ValueError('not isinstance(host_conninfo, str)')
            
            if host_params is not None and not isinstance(host_params, dict):
                raise ValueError('not isinstance(host_params, dict)')
            
            host_list.append({
                'name': host_name,
                'type': host_type,
                'conninfo': host_conninfo,
                'params': host_params,
            })
        
        self.hosts_file_path = hosts_file_path
        self.host_list = host_list

class InitDescr:
    _load_utils = LoadUtils
    
    file_name = 'init.yaml'
    
    def load(self, init_file_path, include_list):
        init_file_dir = os.path.dirname(init_file_path)
        
        with self._load_utils.check_and_open_for_r(init_file_path, include_list) as fd:
            doc = yaml.safe_load(fd)
        
        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')
        
        init_elem = doc['init']
        
        if not isinstance(init_elem, dict):
            raise ValueError('not isinstance(init_elem, dict)')
        
        include_elem = init_elem.get('include')
        first_elem = init_elem.get('first')
        last_elem = init_elem.get('last')
        sql = init_elem.get('sql')
        
        self._load_utils.check_include_elem(include_elem, first_elem, last_elem)
        
        if sql is not None and not isinstance(sql, str):
            raise ValueError('not isinstance(sql, str')
        
        def sql_filt_func(file_path):
            return file_path.endswith('.sql')
        
        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    init_file_dir, include_elem, first_elem, last_elem,
                    sql_filt_func,
                )
        
        self.init_file_path = init_file_path
        self.include_list = include_list
        self.file_path_list = file_path_list
        self.first_file_path_list = first_file_path_list
        self.last_file_path_list = last_file_path_list
        self.sql = sql
    
    def read_sql(self):
        yield from self._load_utils.read_content(
            self.file_path_list,
            self.first_file_path_list,
            self.last_file_path_list,
            self.sql,
            self.include_list,
        )

class SchemaDescr:
    _load_utils = LoadUtils
    
    file_name = 'schema.yaml'
    
    def load(self, schema_file_path, include_list):
        schema_file_dir = os.path.dirname(schema_file_path)
        
        with self._load_utils.check_and_open_for_r(schema_file_path, include_list) as fd:
            doc = yaml.safe_load(fd)
        
        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')
        
        schema_elem = doc['schema']
        
        if not isinstance(schema_elem, dict):
            raise ValueError('not isinstance(schema_elem, dict)')
        
        schema_name = schema_elem['name']
        schema_type = schema_elem['type']
        owner = schema_elem['owner']
        grant_elem = schema_elem.get('grant')
        include_elem = schema_elem.get('include')
        first_elem = schema_elem.get('first')
        last_elem = schema_elem.get('last')
        sql = schema_elem.get('sql')
        
        if not isinstance(schema_name, str):
            raise ValueError('not isinstance(schema_name, str)')
        
        if not isinstance(schema_type, str):
            raise ValueError('not isinstance(schema_type, str)')
        
        if not isinstance(owner, str):
            raise ValueError('not isinstance(owner, str)')
        
        if grant_elem is not None and not isinstance(grant_elem, (list, str)):
            raise ValueError('not isinstance(grant_elem, (list, str)')
        
        self._load_utils.check_include_elem(include_elem, first_elem, last_elem)
        
        if sql is not None and not isinstance(sql, str):
            raise ValueError('not isinstance(sql, str')
        
        if grant_elem is None:
            grant_list = None
        elif isinstance(grant_elem, str):
            grant_list = [grant_elem]
        else:
            grant_list = []
            
            for grant in grant_elem:
                if not isinstance(grant, str):
                    raise ValueError('not isinstance(grant, str)')
                
                grant_list.append(grant)
        
        def sql_filt_func(file_path):
            return file_path.endswith('.sql')
        
        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    schema_file_dir, include_elem, first_elem, last_elem,
                    sql_filt_func,
                )
        
        self.schema_file_path = schema_file_path
        self.include_list = include_list
        self.schema_name = schema_name
        self.schema_type = schema_type
        self.owner = owner
        self.grant_list = grant_list
        self.file_path_list = file_path_list
        self.first_file_path_list = first_file_path_list
        self.last_file_path_list = last_file_path_list
        self.sql = sql
    
    def read_sql(self):
        yield from self._load_utils.read_content(
            self.file_path_list,
            self.first_file_path_list,
            self.last_file_path_list,
            self.sql,
            self.include_list,
        )

class SchemasDescr:
    _load_utils = LoadUtils
    _init_descr_class = InitDescr
    _schema_descr_class = SchemaDescr
    
    file_name = 'schemas.yaml'
    
    def load(self, schemas_file_path, include_list):
        schemas_file_dir = os.path.dirname(schemas_file_path)
        
        with self._load_utils.check_and_open_for_r(schemas_file_path, include_list) as fd:
            doc = yaml.safe_load(fd)
        
        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')
        
        schemas_elem = doc['schemas']
        
        if not isinstance(schemas_elem, dict):
            raise ValueError('not isinstance(schemas_elem, dict)')
        
        schemas_type = schemas_elem['type']
        include_elem = schemas_elem.get('include')
        first_elem = schemas_elem.get('first')
        last_elem = schemas_elem.get('last')
        
        if not isinstance(schemas_type, str):
            raise ValueError('not isinstance(schemas_type, str)')
        
        self._load_utils.check_include_elem(include_elem, first_elem, last_elem)
        
        def init_filt_func(file_path):
            init_file_path = os.path.realpath(os.path.join(
                file_path,
                self._init_descr_class.file_name,
            ))
            
            return os.path.isfile(init_file_path)
        
        def schema_filt_func(file_path):
            schema_file_path = os.path.realpath(os.path.join(
                file_path,
                self._schema_descr_class.file_name,
            ))
            
            return os.path.isfile(schema_file_path)
        
        def filt_func(file_path):
            return init_filt_func(file_path) or schema_filt_func(file_path)
        
        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    schemas_file_dir, include_elem, first_elem, last_elem,
                    filt_func,
                )
        
        init = None
        var_schema_list = []
        func_schema_list = []
        schema_name_set = set()
        
        for file_path in first_file_path_list + file_path_list + last_file_path_list:
            if init_filt_func(file_path):
                if init is not None:
                    raise ValueError(
                        '{!r}: non unique init'.format(
                            init_file_path,
                        )
                    )
                
                init_file_path = os.path.realpath(os.path.join(
                    file_path,
                    self._init_descr_class.file_name,
                ))
                
                init_descr = self._init_descr_class()
                
                try:
                    init_descr.load(init_file_path, include_list)
                except (LookupError, ValueError) as e:
                    raise ValueError('{!r}: {!r}: {}'.format(init_file_path, type(e), e)) from e
                except OSError as e:
                    raise OSError('{!r}: {!r}: {}'.format(init_file_path, type(e), e)) from e
                
                init = init_descr
            elif schema_filt_func(file_path):
                schema_file_path = os.path.realpath(os.path.join(
                    file_path,
                    self._schema_descr_class.file_name,
                ))
                
                schema_descr = self._schema_descr_class()
                
                try:
                    schema_descr.load(schema_file_path, include_list)
                except (LookupError, ValueError) as e:
                    raise ValueError('{!r}: {!r}: {}'.format(schema_file_path, type(e), e)) from e
                except OSError as e:
                    raise OSError('{!r}: {!r}: {}'.format(schema_file_path, type(e), e)) from e
                
                if schema_descr.schema_name in schema_name_set:
                    raise ValueError(
                        '{!r}, {!r}: non unique schema_name'.format(
                            schema_descr.schema_name,
                            schema_file_path,
                        )
                    )
                
                schema_name_set.add(schema_descr.schema_name)
                
                if schema_descr.schema_type == 'var':
                    var_schema_list.append(schema_descr)
                elif schema_descr.schema_type == 'func':
                    func_schema_list.append(schema_descr)
                else:
                    raise ValueError(
                        '{!r}, {!r}: unknown schema_type'.format(
                            schema_descr.schema_type,
                            schema_file_path,
                        )
                    )
            else:
                raise AssertionError
        
        self.schemas_file_path = schemas_file_path
        self.include_list = include_list
        self.schemas_type = schemas_type
        self.init = init
        self.var_schema_list = var_schema_list
        self.func_schema_list = func_schema_list

class SettingsDescr:
    _load_utils = LoadUtils
    
    file_name = 'settings.yaml'
    
    def load(self, settings_file_path, include_list):
        settings_file_dir = os.path.dirname(settings_file_path)
        
        with self._load_utils.check_and_open_for_r(settings_file_path, include_list) as fd:
            doc = yaml.safe_load(fd)
        
        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')
        
        settings_elem = doc['settings']
        
        if not isinstance(settings_elem, dict):
            raise ValueError('not isinstance(settings_elem, dict)')
        
        settings_type = settings_elem['type']
        include_elem = settings_elem.get('include')
        first_elem = settings_elem.get('first')
        last_elem = settings_elem.get('last')
        sql = settings_elem.get('sql')
        
        if not isinstance(settings_type, str):
            raise ValueError('not isinstance(settings_type, str)')
        
        self._load_utils.check_include_elem(include_elem, first_elem, last_elem)
        
        if sql is not None and not isinstance(sql, str):
            raise ValueError('not isinstance(sql, str')
        
        def sql_filt_func(file_path):
            return file_path.endswith('.sql')
        
        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    settings_file_dir, include_elem, first_elem, last_elem,
                    sql_filt_func,
                )
        
        self.settings_file_path = settings_file_path
        self.include_list = include_list
        self.settings_type = settings_type
        self.file_path_list = file_path_list
        self.first_file_path_list = first_file_path_list
        self.last_file_path_list = last_file_path_list
        self.sql = sql
    
    def read_sql(self):
        yield from self._load_utils.read_content(
            self.file_path_list,
            self.first_file_path_list,
            self.last_file_path_list,
            self.sql,
            self.include_list,
        )

class MigrationsDescr:
    _load_utils = LoadUtils
    #_migration_descr_class = MigrationDescr
    
    file_name = 'migrations.yaml'
    
    def load(self, cluster_file_path, include_list):
        pass # TODO     ... ... ...

class ClusterDescr:
    _load_utils = LoadUtils
    _schemas_descr_class = SchemasDescr
    _settings_descr_class = SettingsDescr
    _migrations_descr_class = MigrationsDescr
    
    file_name = 'cluster.yaml'
    
    def load(self, cluster_file_path, include_list, settingsMode=None):
        if settingsMode is None:
            settingsMode = False
        
        cluster_file_dir = os.path.dirname(cluster_file_path)
        
        with self._load_utils.check_and_open_for_r(cluster_file_path, include_list) as fd:
            doc = yaml.safe_load(fd)
        
        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')
        
        cluster_elem = doc['cluster']
        
        if not isinstance(cluster_elem, dict):
            raise ValueError('not isinstance(cluster_elem, dict)')
        
        if settingsMode:
            revision = None
            compatible_elem = cluster_elem['compatible']
        else:
            revision = cluster_elem['revision']
            compatible_list = []
        
        include_elem = cluster_elem.get('include')
        first_elem = cluster_elem.get('first')
        last_elem = cluster_elem.get('last')
        
        if settingsMode:
            if not isinstance(compatible_elem, (list, str)):
                raise ValueError('not isinstance(compatible_elem, (list, str)')
            
            if isinstance(compatible_elem, str):
                compatible_list = [compatible_elem]
            else:
                compatible_list = []
                
                for compatible_item_elem in compatible_elem:
                    if not isinstance(compatible_item_elem, str):
                        raise ValueError('not isinstance(compatible_item_elem, str)')
                    
                    compatible_list.append(compatible_item_elem)
        else:
            if not isinstance(revision, str):
                raise ValueError('not isinstance(revision, str)')
            
            compatible_list = []
        
        self._load_utils.check_include_elem(include_elem, first_elem, last_elem)
        
        def schemas_filt_func(file_path):
            schemas_file_path = os.path.realpath(os.path.join(
                file_path,
                self._schemas_descr_class.file_name,
            ))
            
            return os.path.isfile(schemas_file_path)
        
        def settings_filt_func(file_path):
            settings_file_path = os.path.realpath(os.path.join(
                file_path,
                self._settings_descr_class.file_name,
            ))
            
            return os.path.isfile(settings_file_path)
        
        def migrations_filt_func(file_path):
            migrations_file_path = os.path.realpath(os.path.join(
                file_path,
                self._migrations_descr_class.file_name,
            ))
            
            return os.path.isfile(migrations_file_path)
        
        if settingsMode:
            def filt_func(file_path):
                return settings_filt_func(file_path) or migrations_filt_func(file_path)
        else:
            def filt_func(file_path):
                return schemas_filt_func(file_path) or migrations_filt_func(file_path)
        
        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    cluster_file_dir, include_elem, first_elem, last_elem,
                    filt_func,
                )
        
        schemas_list = []
        settings_list = []
        migrations = None
        schemas_type_set = set()
        settings_type_set = set()
        
        for file_path in first_file_path_list + file_path_list + last_file_path_list:
            if schemas_filt_func(file_path):
                schemas_file_path = os.path.realpath(os.path.join(
                    file_path,
                    self._schemas_descr_class.file_name,
                ))
                
                schemas_descr = self._schemas_descr_class()
                
                try:
                    schemas_descr.load(schemas_file_path, include_list)
                except (LookupError, ValueError) as e:
                    raise ValueError('{!r}: {!r}: {}'.format(schemas_file_path, type(e), e)) from e
                except OSError as e:
                    raise OSError('{!r}: {!r}: {}'.format(schemas_file_path, type(e), e)) from e
                
                if schemas_descr.schemas_type in schemas_type_set:
                    raise ValueError(
                        '{!r}, {!r}: non unique schemas_type'.format(
                            schemas_descr.schemas_type,
                            schemas_file_path,
                        )
                    )
                
                schemas_type_set.add(schemas_descr.schemas_type)
                schemas_list.append(schemas_descr)
            elif settings_filt_func(file_path):
                settings_file_path = os.path.realpath(os.path.join(
                    file_path,
                    self._settings_descr_class.file_name,
                ))
                
                settings_descr = self._settings_descr_class()
                
                try:
                    settings_descr.load(settings_file_path, include_list)
                except (LookupError, ValueError) as e:
                    raise ValueError('{!r}: {!r}: {}'.format(settings_file_path, type(e), e)) from e
                except OSError as e:
                    raise OSError('{!r}: {!r}: {}'.format(settings_file_path, type(e), e)) from e
                
                if settings_descr.settings_type in settings_type_set:
                    raise ValueError(
                        '{!r}, {!r}: non unique settings_type'.format(
                            settings_descr.settings_type,
                            settings_file_path,
                        )
                    )
                
                settings_type_set.add(settings_descr.settings_type)
                settings_list.append(settings_descr)
            elif migrations_filt_func(file_path):
                if migrations is not None:
                    raise ValueError(
                        '{!r}: non unique migrations'.format(
                            migrations_file_path,
                        )
                    )
                
                migrations_file_path = os.path.realpath(os.path.join(
                    file_path,
                    self._migrations_descr_class.file_name,
                ))
                
                migrations_descr = self._migrations_descr_class()
                
                try:
                    migrations_descr.load(migrations_file_path, include_list)
                except (LookupError, ValueError) as e:
                    raise ValueError('{!r}: {!r}: {}'.format(migrations_file_path, type(e), e)) from e
                except OSError as e:
                    raise OSError('{!r}: {!r}: {}'.format(migrations_file_path, type(e), e)) from e
                
                migrations = migrations_descr
            else:
                raise AssertionError
        
        self.cluster_file_path = cluster_file_path
        self.include_list = include_list
        self.revision = revision
        self.compatible_list = compatible_list
        self.schemas_list = schemas_list
        self.migrations = migrations
        self.settings = settings_list = settings_list
