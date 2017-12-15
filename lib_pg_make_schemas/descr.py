# -*- mode: python; coding: utf-8 -*-

import contextlib
import os, os.path
import yaml

class LoadUtils:
    @classmethod
    def check_for_real(self, file_path):
        if os.path.islink(file_path):
            raise ValueError('{!r}: this file is link'.format(file_path))
        
        real_file_path = os.path.realpath(file_path)
        
        if file_path != real_file_path:
            raise ValueError('{!r}, {!r}: checking for real is not passed'.format(
                file_path, real_file_path
            ))
    
    @classmethod
    def norm_path_join(self, path1, path2):
        path = os.path.join(path1, path2)
        
        if path != os.path.normpath(path) or \
                os.path.normpath(path) == os.path.normpath(path2):
            raise ValueError('{!r}, {!r}: this path joining is not normal'.format(path1, path2))
        
        return path
    
    @classmethod
    def check_include_elem(self, include_elem, first_elem, last_elem):
        if include_elem is not None and not isinstance(include_elem, (list, str)):
            raise ValueError('not isinstance(include_elem, (list, str))')
        
        if first_elem is not None and not isinstance(first_elem, (list, str)):
            raise ValueError('not isinstance(first_elem, (list, str))')
        
        if last_elem is not None and not isinstance(last_elem, (list, str)):
            raise ValueError('not isinstance(last_elem, (list, str))')
    
    @classmethod
    def load_file_path_list(self, file_dir, include_elem, first_elem, last_elem, filt_func):
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
                
                path = self.norm_path_join(file_dir, include_item_elem)
                
                path_list.append(path)
        
        path_list.append(file_dir)
        
        for path in path_list:
            for f in sorted(d.name for d in os.scandir(path)):
                file_path = self.norm_path_join(path, f)
                
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
                        file_path = self.norm_path_join(path, ordered_item_elem)
                        
                        if file_path not in file_path_list:
                            continue
                        
                        file_is_used = True
                        file_path_list.remove(file_path)
                        ordered_file_path_list.append(file_path)
                    
                    if not file_is_used:
                        raise ValueError('{!r}: this file is not used'.format(file_path))
        
        return file_path_list, first_file_path_list, last_file_path_list

class HostsDescr:
    _load_utils = LoadUtils

    def load(self, file_path):
        self._load_utils.check_for_real(file_path)
        
        with open(file_path, encoding='utf-8') as fd:
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
        
        self.host_list = host_list

class SchemaDescr:
    _load_utils = LoadUtils
    
    file_name = 'schema.yaml'

    def load(self, file_path):
        self._load_utils.check_for_real(file_path)
        
        file_dir = os.path.dirname(file_path)
        
        with open(file_path, encoding='utf-8') as fd:
            doc = yaml.safe_load(fd)
        
        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')
        
        schema_elem = doc['schema']
        
        if not isinstance(schema_elem, dict):
            raise ValueError('not isinstance(doc, schema_elem)')
        
        schema_name = schema_elem['name']
        schema_type = schema_elem['type']
        owner = schema_elem['owner']
        grant_elem = schema_elem.get('grant')
        include_elem = schema_elem.get('include')
        first_elem = schema_elem.get('first')
        last_elem = schema_elem.get('last')
        sql = schema_elem['sql']
        
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
                    file_dir, include_elem, first_elem, last_elem, sql_filt_func
                )
        
        self.schema_name = schema_name
        self.schema_type = schema_type
        self.owner = owner
        self.grant_list = grant_list
        self.file_path_list = file_path_list
        self.first_file_path_list = first_file_path_list
        self.last_file_path_list = last_file_path_list
        self.sql = sql

class SchemasDescr:
    _load_utils = LoadUtils
    _schema_descr_class = SchemaDescr
    
    file_name = 'schemas.yaml'

    def load(self, file_path):
        self._load_utils.check_for_real(file_path)
        
        file_dir = os.path.dirname(file_path)
        
        with open(file_path, encoding='utf-8') as fd:
            doc = yaml.safe_load(fd)
        
        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')
        
        schemas_elem = doc['schemas']
        
        if not isinstance(schemas_elem, dict):
            raise ValueError('not isinstance(doc, schemas_elem)')
        
        schemas_type = schemas_elem['type']
        include_elem = schemas_elem.get('include')
        first_elem = schemas_elem.get('first')
        last_elem = schemas_elem.get('last')
        
        if not isinstance(schemas_type, str):
            raise ValueError('not isinstance(schemas_type, str)')
        
        self._load_utils.check_include_elem(include_elem, first_elem, last_elem)
        
        def schema_filt_func(file_path):
            schema_file_path = self._load_utils.norm_path_join(
                file_path,
                self._schema_descr_class.file_name,
            )
            
            return os.path.isfile(schema_file_path)
        
        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    file_dir, include_elem, first_elem, last_elem, schema_filt_func
                )
        
        var_schema_list = []
        func_schema_list = []
        schema_name_set = set()
        
        for file_path in first_file_path_list + file_path_list + last_file_path_list:
            schema_file_path = self._load_utils.norm_path_join(
                file_path,
                self._schema_descr_class.file_name,
            )
            
            schema_descr = self._schema_descr_class()
            
            try:
                schema_descr.load(schema_file_path)
            except (LookupError, ValueError) as e:
                raise ValueError('{!r}: {!r}: {}'.format(schema_file_path, type(e), e)) from e
            
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
        
        self.schemas_type = schemas_type
        self.var_schema_list = var_schema_list
        self.func_schema_list = func_schema_list

class ClusterDescr:
    _load_utils = LoadUtils
    _schemas_descr_class = SchemasDescr
    
    file_name = 'cluster.yaml'

    def load(self, file_path):
        self._load_utils.check_for_real(file_path)
        
        file_dir = os.path.dirname(file_path)
        
        with open(file_path, encoding='utf-8') as fd:
            doc = yaml.safe_load(fd)
        
        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')
        
        cluster_elem = doc['cluster']
        
        if not isinstance(cluster_elem, dict):
            raise ValueError('not isinstance(doc, cluster_elem)')
        
        revision = cluster_elem['revision']
        include_elem = cluster_elem.get('include')
        first_elem = cluster_elem.get('first')
        last_elem = cluster_elem.get('last')
        
        if not isinstance(revision, str):
            raise ValueError('not isinstance(revision, str)')
        
        self._load_utils.check_include_elem(include_elem, first_elem, last_elem)
        
        def schemas_filt_func(file_path):
            schemas_file_path = self._load_utils.norm_path_join(
                file_path,
                self._schemas_descr_class.file_name,
            )
            
            return os.path.isfile(schemas_file_path)
        
        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    file_dir, include_elem, first_elem, last_elem, schemas_filt_func
                )
        
        schemas_list = []
        schemas_type_set = set()
        
        for file_path in first_file_path_list + file_path_list + last_file_path_list:
            schemas_file_path = self._load_utils.norm_path_join(
                file_path,
                self._schemas_descr_class.file_name,
            )
            
            schemas_descr = self._schemas_descr_class()
            
            try:
                schemas_descr.load(schemas_file_path)
            except (LookupError, ValueError) as e:
                raise ValueError('{!r}: {!r}: {}'.format(schemas_file_path, type(e), e)) from e
            
            if schemas_descr.schemas_type in schemas_type_set:
                raise ValueError(
                    '{!r}, {!r}: non unique schemas_type'.format(
                        schemas_descr.schemas_type,
                        schemas_file_path,
                    )
                )
            
            schemas_type_set.add(schemas_descr.schemas_type)
            schemas_list.append(schemas_descr)
        
        self.revision = revision
        self.schemas_list = schemas_list
