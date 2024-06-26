import os, os.path
import yaml
import re

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
    def yaml_safe_load(cls, fd):
        return yaml.safe_load(fd)

    @classmethod
    def resolve_include_ref(cls, include, include_ref_map):
        m = re.fullmatch(
            r'(\$\{([A-Za-z0-9_]+)\}|\$([A-Za-z0-9_]+))(.*)',
            include,
            flags=re.S,
        )

        if m is None:
            return include

        ref_name = m.group(2) if m.group(2) is not None else m.group(3)
        other_value = m.group(4)

        ref_value = include_ref_map.get(ref_name)

        if ref_value is None:
            raise ValueError('{!r}: undefined include-reference'.format(ref_name))

        resolved_include = '{}{}'.format(ref_value, other_value)

        return resolved_include

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
                include_elem, include_ref_map,
                first_elem, last_elem, filt_func,
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

                resolved_include = cls.resolve_include_ref(
                    include_item_elem,
                    include_ref_map,
                )

                path = os.path.realpath(os.path.join(
                    file_dir,
                    resolved_include,
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
                inline, inline_path,
                include_list,
            ):
        if first_file_path_list is not None:
            for file_path in first_file_path_list:
                info = {
                    'file_path': os.path.relpath(file_path, start=include_list[-1]),
                    'file_path_type': 'first',
                }

                with cls.check_and_open_for_r(file_path, include_list) as fd:
                    yield fd.read(), info

        if file_path_list is not None:
            for file_path in file_path_list:
                info = {
                    'file_path': os.path.relpath(file_path, start=include_list[-1]),
                    'file_path_type': 'regular',
                }

                with cls.check_and_open_for_r(file_path, include_list) as fd:
                    yield fd.read(), info

        if inline is not None:
            info = {
                'file_path': os.path.relpath(inline_path, start=include_list[-1])
                        if inline_path is not None else None,
                'file_path_type': 'inline',
            }

            yield inline, info

        if last_file_path_list is not None:
            for file_path in last_file_path_list:
                info = {
                    'file_path': os.path.relpath(file_path, start=include_list[-1]),
                    'file_path_type': 'last',
                }

                with cls.check_and_open_for_r(file_path, include_list) as fd:
                    yield fd.read(), info

class InitDescr:
    _load_utils = LoadUtils

    file_name = 'init.yaml'

    def load(self, init_file_path, include_list, include_ref_map,
            virtual_doc=None):
        init_file_dir = os.path.dirname(init_file_path)

        if virtual_doc is not None:
            doc = virtual_doc
        else:
            with self._load_utils.check_and_open_for_r(init_file_path, include_list) as fd:
                doc = self._load_utils.yaml_safe_load(fd)

        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')

        init_elem = doc['init']

        if init_elem is None:
            init_elem = {}

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
                    init_file_dir, include_elem, include_ref_map,
                    first_elem, last_elem, sql_filt_func,
                )

        self.virtual_doc = virtual_doc
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
            self.sql, self.init_file_path,
            self.include_list,
        )

class SchemaDescr:
    _load_utils = LoadUtils

    file_name = 'schema.yaml'

    def load(self, schema_file_path, include_list, include_ref_map,
            virtual_doc=None):
        schema_file_dir = os.path.dirname(schema_file_path)

        if virtual_doc is not None:
            doc = virtual_doc
        else:
            with self._load_utils.check_and_open_for_r(schema_file_path, include_list) as fd:
                doc = self._load_utils.yaml_safe_load(fd)

        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')

        schema_elem = doc['schema']

        if schema_elem is None:
            schema_elem = {}

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
                    schema_file_dir, include_elem, include_ref_map,
                    first_elem, last_elem, sql_filt_func,
                )

        self.virtual_doc = virtual_doc
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
            self.sql, self.schema_file_path,
            self.include_list,
        )

class LateDescr:
    _load_utils = LoadUtils

    file_name = 'late.yaml'

    def load(self, late_file_path, include_list, include_ref_map,
            virtual_doc=None):
        late_file_dir = os.path.dirname(late_file_path)

        if virtual_doc is not None:
            doc = virtual_doc
        else:
            with self._load_utils.check_and_open_for_r(late_file_path, include_list) as fd:
                doc = self._load_utils.yaml_safe_load(fd)

        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')

        late_elem = doc['late']

        if late_elem is None:
            late_elem = {}

        if not isinstance(late_elem, dict):
            raise ValueError('not isinstance(late_elem, dict)')

        include_elem = late_elem.get('include')
        first_elem = late_elem.get('first')
        last_elem = late_elem.get('last')
        sql = late_elem.get('sql')

        self._load_utils.check_include_elem(include_elem, first_elem, last_elem)

        if sql is not None and not isinstance(sql, str):
            raise ValueError('not isinstance(sql, str')

        def sql_filt_func(file_path):
            return file_path.endswith('.sql')

        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    late_file_dir, include_elem, include_ref_map,
                    first_elem, last_elem, sql_filt_func,
                )

        self.virtual_doc = virtual_doc
        self.late_file_path = late_file_path
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
            self.sql, self.late_file_path,
            self.include_list,
        )

class SafeguardDescr:
    _load_utils = LoadUtils

    file_name = 'safeguard.yaml'

    def load(self, safeguard_file_path, include_list, include_ref_map,
            virtual_doc=None):
        safeguard_file_dir = os.path.dirname(safeguard_file_path)

        if virtual_doc is not None:
            doc = virtual_doc
        else:
            with self._load_utils.check_and_open_for_r(safeguard_file_path, include_list) as fd:
                doc = self._load_utils.yaml_safe_load(fd)

        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')

        safeguard_elem = doc['safeguard']

        if safeguard_elem is None:
            safeguard_elem = {}

        if not isinstance(safeguard_elem, dict):
            raise ValueError('not isinstance(safeguard_elem, dict)')

        include_elem = safeguard_elem.get('include')
        first_elem = safeguard_elem.get('first')
        last_elem = safeguard_elem.get('last')
        sql = safeguard_elem.get('sql')

        self._load_utils.check_include_elem(include_elem, first_elem, last_elem)

        if sql is not None and not isinstance(sql, str):
            raise ValueError('not isinstance(sql, str')

        def sql_filt_func(file_path):
            return file_path.endswith('.sql')

        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    safeguard_file_dir, include_elem, include_ref_map,
                    first_elem, last_elem, sql_filt_func,
                )

        self.virtual_doc = virtual_doc
        self.safeguard_file_path = safeguard_file_path
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
            self.sql, self.safeguard_file_path,
            self.include_list,
        )

class SchemasDescr:
    _load_utils = LoadUtils
    _init_descr_class = InitDescr
    _schema_descr_class = SchemaDescr
    _late_descr_class = LateDescr
    _safeguard_descr_class = SafeguardDescr

    file_name = 'schemas.yaml'

    def load(self, schemas_file_path, include_list, include_ref_map,
            virtual_doc=None, schemas_type=None):
        schemas_file_dir = os.path.dirname(schemas_file_path)

        if virtual_doc is not None:
            doc = virtual_doc
        else:
            with self._load_utils.check_and_open_for_r(schemas_file_path, include_list) as fd:
                doc = self._load_utils.yaml_safe_load(fd)

        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')

        schemas_elem = doc['schemas']

        if schemas_elem is None:
            schemas_elem = {}

        if not isinstance(schemas_elem, dict):
            raise ValueError('not isinstance(schemas_elem, dict)')

        schemas_type = schemas_elem.get('type', schemas_type)
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

        def late_filt_func(file_path):
            late_file_path = os.path.realpath(os.path.join(
                file_path,
                self._late_descr_class.file_name,
            ))

            return os.path.isfile(late_file_path)

        def safeguard_filt_func(file_path):
            safeguard_file_path = os.path.realpath(os.path.join(
                file_path,
                self._safeguard_descr_class.file_name,
            ))

            return os.path.isfile(safeguard_file_path)

        def filt_func(file_path):
            return init_filt_func(file_path) or schema_filt_func(file_path) or \
                    late_filt_func(file_path) or safeguard_filt_func(file_path)

        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    schemas_file_dir, include_elem, include_ref_map,
                    first_elem, last_elem, filt_func,
                )

        init = None
        var_schema_list = []
        late = None
        func_schema_list = []
        safeguard = None
        schema_name_set = set()

        for file_path in first_file_path_list + file_path_list + last_file_path_list:
            if init_filt_func(file_path):
                if init is not None:
                    raise ValueError(
                        '{!r}: non unique init'.format(
                            init_file_path,
                        ),
                    )

                init_file_path = os.path.realpath(os.path.join(
                    file_path,
                    self._init_descr_class.file_name,
                ))

                init_descr = self._init_descr_class()

                try:
                    init_descr.load(init_file_path, include_list, include_ref_map)
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
                    schema_descr.load(schema_file_path, include_list, include_ref_map)
                except (LookupError, ValueError) as e:
                    raise ValueError('{!r}: {!r}: {}'.format(schema_file_path, type(e), e)) from e
                except OSError as e:
                    raise OSError('{!r}: {!r}: {}'.format(schema_file_path, type(e), e)) from e

                if schema_descr.schema_name in schema_name_set:
                    raise ValueError(
                        '{!r}, {!r}: non unique schema_name'.format(
                            schema_descr.schema_name,
                            schema_file_path,
                        ),
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
                        ),
                    )
            elif late_filt_func(file_path):
                if late is not None:
                    raise ValueError(
                        '{!r}: non unique late'.format(
                            late_file_path,
                        ),
                    )

                late_file_path = os.path.realpath(os.path.join(
                    file_path,
                    self._late_descr_class.file_name,
                ))

                late_descr = self._late_descr_class()

                try:
                    late_descr.load(late_file_path, include_list, include_ref_map)
                except (LookupError, ValueError) as e:
                    raise ValueError('{!r}: {!r}: {}'.format(late_file_path, type(e), e)) from e
                except OSError as e:
                    raise OSError('{!r}: {!r}: {}'.format(late_file_path, type(e), e)) from e

                late = late_descr
            elif safeguard_filt_func(file_path):
                if safeguard is not None:
                    raise ValueError(
                        '{!r}: non unique safeguard'.format(
                            safeguard_file_path,
                        ),
                    )

                safeguard_file_path = os.path.realpath(os.path.join(
                    file_path,
                    self._safeguard_descr_class.file_name,
                ))

                safeguard_descr = self._safeguard_descr_class()

                try:
                    safeguard_descr.load(safeguard_file_path, include_list, include_ref_map)
                except (LookupError, ValueError) as e:
                    raise ValueError('{!r}: {!r}: {}'.format(safeguard_file_path, type(e), e)) from e
                except OSError as e:
                    raise OSError('{!r}: {!r}: {}'.format(safeguard_file_path, type(e), e)) from e

                safeguard = safeguard_descr
            else:
                raise AssertionError

        self.virtual_doc = virtual_doc
        self.schemas_file_path = schemas_file_path
        self.include_list = include_list
        self.schemas_type = schemas_type
        self.init = init
        self.var_schema_list = var_schema_list
        self.late = late
        self.func_schema_list = func_schema_list
        self.safeguard = safeguard

class SettingsDescr:
    _load_utils = LoadUtils

    file_name = 'settings.yaml'

    def load(self, settings_file_path, include_list, include_ref_map,
            virtual_doc=None, settings_type=None):
        settings_file_dir = os.path.dirname(settings_file_path)

        if virtual_doc is not None:
            doc = virtual_doc
        else:
            with self._load_utils.check_and_open_for_r(settings_file_path, include_list) as fd:
                doc = self._load_utils.yaml_safe_load(fd)

        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')

        settings_elem = doc['settings']

        if settings_elem is None:
            settings_elem = {}

        if not isinstance(settings_elem, dict):
            raise ValueError('not isinstance(settings_elem, dict)')

        settings_type = settings_elem.get('type', settings_type)
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
                    settings_file_dir, include_elem, include_ref_map,
                    first_elem, last_elem, sql_filt_func,
                )

        self.virtual_doc = virtual_doc
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
            self.sql, self.settings_file_path,
            self.include_list,
        )

class UpgradeDescr:
    _load_utils = LoadUtils

    file_name = 'upgrade.yaml'

    def load(self, upgrade_file_path, include_list, include_ref_map,
            virtual_doc=None):
        upgrade_file_dir = os.path.dirname(upgrade_file_path)

        if virtual_doc is not None:
            doc = virtual_doc
        else:
            with self._load_utils.check_and_open_for_r(upgrade_file_path, include_list) as fd:
                doc = self._load_utils.yaml_safe_load(fd)

        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')

        upgrade_elem = doc['upgrade']

        if upgrade_elem is None:
            upgrade_elem = {}

        if not isinstance(upgrade_elem, dict):
            raise ValueError('not isinstance(upgrade_elem, dict)')

        upgrade_type = upgrade_elem['type']
        include_elem = upgrade_elem.get('include')
        first_elem = upgrade_elem.get('first')
        last_elem = upgrade_elem.get('last')
        sql = upgrade_elem.get('sql')

        if not isinstance(upgrade_type, str):
            raise ValueError('not isinstance(upgrade_type, str)')

        self._load_utils.check_include_elem(include_elem, first_elem, last_elem)

        if sql is not None and not isinstance(sql, str):
            raise ValueError('not isinstance(sql, str')

        def sql_filt_func(file_path):
            return file_path.endswith('.sql')

        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    upgrade_file_dir, include_elem, include_ref_map,
                    first_elem, last_elem, sql_filt_func,
                )

        self.virtual_doc = virtual_doc
        self.upgrade_file_path = upgrade_file_path
        self.include_list = include_list
        self.upgrade_type = upgrade_type
        self.file_path_list = file_path_list
        self.first_file_path_list = first_file_path_list
        self.last_file_path_list = last_file_path_list
        self.sql = sql

    def read_sql(self):
        yield from self._load_utils.read_content(
            self.file_path_list,
            self.first_file_path_list,
            self.last_file_path_list,
            self.sql, self.upgrade_file_path,
            self.include_list,
        )

class MigrationDescr:
    _load_utils = LoadUtils
    _upgrade_descr_class = UpgradeDescr

    file_name = 'migration.yaml'

    def load(self, migration_file_path, include_list, include_ref_map,
            virtual_doc=None, migration_type=None):
        migration_file_dir = os.path.dirname(migration_file_path)

        if virtual_doc is not None:
            doc = virtual_doc
        else:
            with self._load_utils.check_and_open_for_r(migration_file_path, include_list) as fd:
                doc = self._load_utils.yaml_safe_load(fd)

        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')

        migration_elem = doc['migration']

        if migration_elem is None:
            migration_elem = {}

        if not isinstance(migration_elem, dict):
            raise ValueError('not isinstance(migration_elem, dict)')

        migration_type = migration_elem.get('type', migration_type)
        revision = migration_elem['revision']
        compatible_elem = migration_elem['compatible']

        include_elem = migration_elem.get('include')
        first_elem = migration_elem.get('first')
        last_elem = migration_elem.get('last')

        if migration_type is not None and not isinstance(migration_type, str):
            raise ValueError('not isinstance(migration_type, str)')

        if not isinstance(revision, str):
            raise ValueError('not isinstance(revision, str)')

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

        self._load_utils.check_include_elem(include_elem, first_elem, last_elem)

        upgrade_list = []

        if migration_type is not None:
            upgrade_sql = migration_elem.get('sql')

            upgrade_file_path = migration_file_path
            upgrade_virtual_doc = {
                'upgrade': {
                    'type': migration_type,
                    'include': include_elem,
                    'first': first_elem,
                    'last': last_elem,
                    'sql': upgrade_sql,
                }
            }

            upgrade_descr = self._upgrade_descr_class()

            try:
                upgrade_descr.load(upgrade_file_path, include_list,
                        include_ref_map, virtual_doc=upgrade_virtual_doc)
            except (LookupError, ValueError) as e:
                raise ValueError('{!r}: {!r}: {}'.format(upgrade_file_path, type(e), e)) from e
            except OSError as e:
                raise OSError('{!r}: {!r}: {}'.format(upgrade_file_path, type(e), e)) from e

            upgrade_list.append(upgrade_descr)
        else:
            def upgrade_filt_func(file_path):
                upgrade_file_path = os.path.realpath(os.path.join(
                    file_path,
                    self._upgrade_descr_class.file_name,
                ))

                return os.path.isfile(upgrade_file_path)

            file_path_list, first_file_path_list, last_file_path_list = \
                    self._load_utils.load_file_path_list(
                        migration_file_dir, include_elem, include_ref_map,
                        first_elem, last_elem, upgrade_filt_func,
                    )

            upgrade_type_set = set()

            for file_path in first_file_path_list + file_path_list + last_file_path_list:
                upgrade_file_path = os.path.realpath(os.path.join(
                    file_path,
                    self._upgrade_descr_class.file_name,
                ))

                upgrade_descr = self._upgrade_descr_class()

                try:
                    upgrade_descr.load(upgrade_file_path, include_list, include_ref_map)
                except (LookupError, ValueError) as e:
                    raise ValueError('{!r}: {!r}: {}'.format(upgrade_file_path, type(e), e)) from e
                except OSError as e:
                    raise OSError('{!r}: {!r}: {}'.format(upgrade_file_path, type(e), e)) from e

                if upgrade_descr.upgrade_type in upgrade_type_set:
                    raise ValueError(
                        '{!r}, {!r}: non unique upgrade_type'.format(
                            upgrade_descr.upgrade_type,
                            upgrade_file_path,
                        ),
                    )

                upgrade_type_set.add(upgrade_descr.upgrade_type)
                upgrade_list.append(upgrade_descr)

        self.virtual_doc = virtual_doc
        self.migration_file_path = migration_file_path
        self.include_list = include_list
        self.migration_type = migration_type
        self.revision = revision
        self.compatible_list = compatible_list
        self.upgrade_list = upgrade_list

class MigrationsDescr:
    _load_utils = LoadUtils
    _migration_descr_class = MigrationDescr

    file_name = 'migrations.yaml'

    def load(self, migrations_file_path, include_list, include_ref_map,
            virtual_doc=None, migrations_type=None):
        migrations_file_dir = os.path.dirname(migrations_file_path)

        if virtual_doc is not None:
            doc = virtual_doc
        else:
            with self._load_utils.check_and_open_for_r(migrations_file_path, include_list) as fd:
                doc = self._load_utils.yaml_safe_load(fd)

        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')

        migrations_elem = doc['migrations']

        if migrations_elem is None:
            migrations_elem = {}

        if not isinstance(migrations_elem, dict):
            raise ValueError('not isinstance(migrations_elem, dict)')

        migrations_type = migrations_elem.get('type', migrations_type)
        include_elem = migrations_elem.get('include')
        first_elem = migrations_elem.get('first')
        last_elem = migrations_elem.get('last')

        if migrations_type is not None and not isinstance(migrations_type, str):
            raise ValueError('not isinstance(migrations_type, str)')

        self._load_utils.check_include_elem(include_elem, first_elem, last_elem)

        def migration_filt_func(file_path):
            migration_file_path = os.path.realpath(os.path.join(
                file_path,
                self._migration_descr_class.file_name,
            ))

            return os.path.isfile(migration_file_path)

        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    migrations_file_dir, include_elem, include_ref_map,
                    first_elem, last_elem, migration_filt_func,
                )

        migration_list = []
        migration_way_set = set()

        for file_path in first_file_path_list + file_path_list + last_file_path_list:
            migration_file_path = os.path.realpath(os.path.join(
                file_path,
                self._migration_descr_class.file_name,
            ))

            migration_descr = self._migration_descr_class()

            try:
                migration_descr.load(migration_file_path, include_list,
                        include_ref_map, migration_type=migrations_type)
            except (LookupError, ValueError) as e:
                raise ValueError('{!r}: {!r}: {}'.format(migration_file_path, type(e), e)) from e
            except OSError as e:
                raise OSError('{!r}: {!r}: {}'.format(migration_file_path, type(e), e)) from e

            if migrations_type is not None and \
                    migration_descr.migration_type != migrations_type:
                raise ValueError(
                    '{!r}, {!r}: migration_type is distinct from migrations_type'.format(
                        migration_descr.migration_type,
                        migration_file_path,
                    ),
                )

            for compatible in migration_descr.compatible_list:
                migration_way = migration_descr.revision, compatible

                if migration_way in migration_way_set:
                    raise ValueError(
                        '{!r}, {!r}: non unique migration_way'.format(
                            migration_way,
                            migration_file_path,
                        ),
                    )

                migration_way_set.add(migration_way)

            migration_list.append(migration_descr)

        self.virtual_doc = virtual_doc
        self.migrations_file_path = migrations_file_path
        self.include_list = include_list
        self.migrations_type = migrations_type
        self.migration_list = migration_list

class ClusterDescr:
    _load_utils = LoadUtils
    _schemas_descr_class = SchemasDescr
    _settings_descr_class = SettingsDescr
    _migrations_descr_class = MigrationsDescr

    file_name = 'cluster.yaml'

    def load(self, cluster_file_path, include_list, include_ref_map,
            virtual_doc=None, cluster_type=None, settings_mode=None):
        if settings_mode is None:
            settings_mode = False

        cluster_file_dir = os.path.dirname(cluster_file_path)

        if virtual_doc is not None:
            doc = virtual_doc
        else:
            with self._load_utils.check_and_open_for_r(cluster_file_path, include_list) as fd:
                doc = self._load_utils.yaml_safe_load(fd)

        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')

        cluster_elem = doc['cluster']

        if cluster_elem is None:
            cluster_elem = {}

        if not isinstance(cluster_elem, dict):
            raise ValueError('not isinstance(cluster_elem, dict)')

        application = cluster_elem['application']
        cluster_type = cluster_elem.get('type', cluster_type)

        if settings_mode:
            revision = None
            compatible_elem = cluster_elem['compatible']
        else:
            revision = cluster_elem['revision']
            compatible_list = []

        include_elem = cluster_elem.get('include')
        first_elem = cluster_elem.get('first')
        last_elem = cluster_elem.get('last')

        if not isinstance(application, str):
            raise ValueError('not isinstance(application, str)')

        if cluster_type is not None and not isinstance(cluster_type, str):
            raise ValueError('not isinstance(cluster_type, str)')

        if settings_mode:
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

        if settings_mode:
            def filt_func(file_path):
                return settings_filt_func(file_path) or migrations_filt_func(file_path)
        else:
            def filt_func(file_path):
                return schemas_filt_func(file_path) or migrations_filt_func(file_path)

        file_path_list, first_file_path_list, last_file_path_list = \
                self._load_utils.load_file_path_list(
                    cluster_file_dir, include_elem, include_ref_map,
                    first_elem, last_elem, filt_func,
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
                    schemas_descr.load(schemas_file_path, include_list,
                            include_ref_map, schemas_type=cluster_type)
                except (LookupError, ValueError) as e:
                    raise ValueError('{!r}: {!r}: {}'.format(schemas_file_path, type(e), e)) from e
                except OSError as e:
                    raise OSError('{!r}: {!r}: {}'.format(schemas_file_path, type(e), e)) from e

                if cluster_type is not None and \
                        schemas_descr.schemas_type != cluster_type:
                    raise ValueError(
                        '{!r}, {!r}: schemas_type is distinct from cluster_type'.format(
                            schemas_descr.schemas_type,
                            schemas_file_path,
                        ),
                    )

                if schemas_descr.schemas_type in schemas_type_set:
                    raise ValueError(
                        '{!r}, {!r}: non unique schemas_type'.format(
                            schemas_descr.schemas_type,
                            schemas_file_path,
                        ),
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
                    settings_descr.load(settings_file_path, include_list,
                            include_ref_map, settings_type=cluster_type)
                except (LookupError, ValueError) as e:
                    raise ValueError('{!r}: {!r}: {}'.format(settings_file_path, type(e), e)) from e
                except OSError as e:
                    raise OSError('{!r}: {!r}: {}'.format(settings_file_path, type(e), e)) from e

                if cluster_type is not None and \
                        settings_descr.settings_type != cluster_type:
                    raise ValueError(
                        '{!r}, {!r}: settings_type is distinct from cluster_type'.format(
                            settings_descr.settings_type,
                            settings_file_path,
                        ),
                    )

                if settings_descr.settings_type in settings_type_set:
                    raise ValueError(
                        '{!r}, {!r}: non unique settings_type'.format(
                            settings_descr.settings_type,
                            settings_file_path,
                        ),
                    )

                settings_type_set.add(settings_descr.settings_type)
                settings_list.append(settings_descr)
            elif migrations_filt_func(file_path):
                if migrations is not None:
                    raise ValueError(
                        '{!r}: non unique migrations'.format(
                            migrations_file_path,
                        ),
                    )

                migrations_file_path = os.path.realpath(os.path.join(
                    file_path,
                    self._migrations_descr_class.file_name,
                ))

                migrations_descr = self._migrations_descr_class()

                try:
                    migrations_descr.load(migrations_file_path, include_list,
                            include_ref_map, migrations_type=cluster_type)
                except (LookupError, ValueError) as e:
                    raise ValueError('{!r}: {!r}: {}'.format(migrations_file_path, type(e), e)) from e
                except OSError as e:
                    raise OSError('{!r}: {!r}: {}'.format(migrations_file_path, type(e), e)) from e

                if cluster_type is not None and \
                        migrations_descr.migrations_type != cluster_type:
                    raise ValueError(
                        '{!r}, {!r}: migrations_type is distinct from cluster_type'.format(
                            migrations_descr.migrations_type,
                            migrations_file_path,
                        ),
                    )

                migrations = migrations_descr
            else:
                raise AssertionError

        self.virtual_doc = virtual_doc
        self.cluster_file_path = cluster_file_path
        self.include_list = include_list
        self.application = application
        self.cluster_type = cluster_type
        self.revision = revision
        self.compatible_list = compatible_list
        self.schemas_list = schemas_list
        self.migrations = migrations
        self.settings_list = settings_list

class HostsDescr:
    _load_utils = LoadUtils

    def _open(self, hosts_file_path):
        return open(hosts_file_path, encoding='utf-8')

    def load(self, hosts_file_path):
        with self._open(hosts_file_path) as fd:
            doc = self._load_utils.yaml_safe_load(fd)

        if not isinstance(doc, dict):
            raise ValueError('not isinstance(doc, dict)')

        hosts_elem = doc['hosts']

        if hosts_elem is None:
            hosts_elem = []

        if not isinstance(hosts_elem, list):
            raise ValueError('not isinstance(hosts_elem, list)')

        host_list = []
        host_name_set = set()
        shared = None

        for host_elem in hosts_elem:
            if not isinstance(host_elem, dict):
                raise ValueError('not isinstance(host_elem, dict)')

            shared_elem = host_elem.get('shared')

            if shared_elem is not None:
                if shared is not None:
                    raise ValueError(
                        '{!r}: non unique shared'.format(
                            hosts_file_path,
                        ),
                    )

                shared = shared_elem

                continue

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

            if host_name in host_name_set:
                raise ValueError(
                    '{!r}, {!r}: non unique host_name'.format(
                        host_name,
                        hosts_file_path,
                    ),
                )

            host_name_set.add(host_name)
            host_list.append({
                'name': host_name,
                'type': host_type,
                'conninfo': host_conninfo,
                'params': host_params,
            })

        self.hosts_file_path = hosts_file_path
        self.host_list = host_list
        self.shared = shared

    def load_pseudo(self, cluster_descr):
        host_list = []

        for schemas in cluster_descr.schemas_list:
            host_name = schemas.schemas_type

            host_list.append({
                'name': host_name,
                'type': host_name,
                'conninfo': None,
                'params': None,
            })

        self.hosts_file_path = '<pseudo-hosts>'
        self.host_list = host_list
        self.shared = None

# vi:ts=4:sw=4:et
