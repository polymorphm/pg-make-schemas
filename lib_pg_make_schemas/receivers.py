import itertools
import psycopg2
from . import pg_notices

class ReceiversError(Exception):
    pass

class SqlFileUtils:
    @classmethod
    def write_header(cls, fd):
        fd.write('-- -*- mode: sql; coding: utf-8 -*-\n\n--begin;\n\n')
        fd.flush()

    @classmethod
    def write_fragment(cls, fd, fragment):
        if isinstance(fragment, tuple):
            fragment_str, fragment_info = fragment
        elif isinstance(fragment, str):
            fragment_str, fragment_info = fragment, {}
        else:
            raise TypeError

        fd.write(fragment_str)
        fd.write('\n\n')
        fd.flush()

    @classmethod
    def write_notices(cls, nfd, notices):
        for notice in notices:
            nfd.write(notice)
            nfd.write('\n')
        nfd.flush()

    @classmethod
    def write_fragment_ok_notice(cls, fd, fragment_i):
        fd.write(
            'do $do$begin raise notice \'fragment {}: ok\'; end$do$;\n\n'.format(
                int(fragment_i),
            ),
        )
        fd.flush()

    @classmethod
    def write_ok_notice(cls, nfd, fragment_i):
        nfd.write(
            '\nfragment {}: ok\n\n'.format(
                int(fragment_i),
            ),
        )
        nfd.flush()

    @classmethod
    def write_footer(cls, fd):
        fd.write('--commit;\n')
        fd.flush()

class Receivers:
    _sql_file_utils = SqlFileUtils

    con_error = psycopg2.Error

    def __init__(self, execute, pretend, output):
        self._execute = execute
        self._pretend = pretend
        self._output = output
        self._con_map = {}
        self._fd_map = {}
        self._nfd_map = {}
        self._frag_cnt_map = {}

        self._notices = self._execute and self._output is not None

    def _connect(self, conninfo):
        con = psycopg2.connect(conninfo)

        if con.autocommit:
            # Psycopg's http://initd.org/psycopg/docs/connection.html says:
            #   """The default is False (manual commit) as per DBAPI specification."""

            raise AssertionError('con.autocommit should not be set into True')

        if self._notices:
            con.notices = pg_notices.PgNotices()

        return con

    def _open(self, output_path):
        return open(output_path, 'w', encoding='utf-8', newline='\n')

    def _make_counter(self, restore_value=None):
        if restore_value is None:
            restore_value = 1

        return itertools.count(restore_value)

    def begin_host(self, hosts_descr, host):
        host_name = host['name']
        host_type = host['type']
        conninfo = host['conninfo']

        if self._execute:
            if host_name in self._con_map:
                raise ValueError(
                    '{!r}, {!r}: non unique host_name'.format(
                        host_name,
                        hosts_descr.hosts_file_path,
                    ),
                )

            if conninfo is None:
                raise ValueError(
                    '{!r}, {!r}: unable to connect to host without its conninfo'.format(
                        host_name,
                        hosts_descr.hosts_file_path,
                    ),
                )

            con = self._connect(conninfo)
            self._con_map[host_name] = con

        if self._output is not None:
            if host_name in self._fd_map:
                raise ValueError(
                    '{!r}, {!r}: non unique host_name'.format(
                        host_name,
                        hosts_descr.hosts_file_path,
                    ),
                )

            output_path = '{}.{}.{}.sql'.format(
                self._output,
                host_name.replace('/', '-').replace('.', '-'),
                host_type.replace('/', '-').replace('.', '-'),
            )

            fd = self._open(output_path)
            self._fd_map[host_name] = fd
            self._frag_cnt_map[host_name] = self._make_counter()

            self._sql_file_utils.write_header(fd)

        if self._notices:
            if host_name in self._nfd_map:
                raise ValueError(
                    '{!r}, {!r}: non unique host_name'.format(
                        host_name,
                        hosts_descr.hosts_file_path,
                    ),
                )

            notices_output_path = '{}.{}.{}.notices'.format(
                self._output,
                host_name.replace('/', '-').replace('.', '-'),
                host_type.replace('/', '-').replace('.', '-'),
            )

            self._nfd_map[host_name] = self._open(notices_output_path)

    def begin(self, hosts_descr, begin_host_verb_func=None):
        for host in hosts_descr.host_list:
            if begin_host_verb_func is not None:
                host_name = host['name']

                begin_host_verb_func(host_name)

            self.begin_host(hosts_descr, host)

    def get_con(self, host_name):
        if self._execute:
            return self._con_map[host_name]

    def look_fragment_i(self, host_name):
        frag_cnt = self._frag_cnt_map.get(host_name)

        if frag_cnt is None:
            return

        fragment_i = next(frag_cnt)
        frag_cnt = self._make_counter(restore_value=fragment_i)
        self._frag_cnt_map[host_name] = frag_cnt

        return fragment_i

    def write_fragment(self, host_name, fragment):
        if self._output is not None:
            fd = self._fd_map[host_name]

            self._sql_file_utils.write_fragment(fd, fragment)

    def write_notices(self, host_name, con):
        if self._notices:
           nfd = self._nfd_map[host_name]
           notices = con.notices.pop_all()

           self._sql_file_utils.write_notices(nfd, notices)

    def write_fragment_ok_notice(self, host_name):
        if self._output is not None:
            fd = self._fd_map[host_name]
            frag_cnt = self._frag_cnt_map[host_name]

            fragment_i = next(frag_cnt)

            self._sql_file_utils.write_fragment_ok_notice(fd, fragment_i)

            if self._notices:
                nfd = self._nfd_map[host_name]
                self._sql_file_utils.write_ok_notice(nfd, fragment_i)

    def execute(self, host_name, fragment):
        self.write_fragment(host_name, fragment)

        if self._execute:
            con = self._con_map[host_name]

            if isinstance(fragment, tuple):
                fragment_str, fragment_info = fragment
            elif isinstance(fragment, str):
                fragment_str, fragment_info = fragment, {}
            else:
                raise TypeError

            try:
                with con.cursor() as cur:
                    cur.execute(fragment_str)
            except self.con_error as e:
                raise ReceiversError(
                        '{!r}: {!r}: {!r}: {}'.format(host_name, fragment_info, type(e), e)) from e
            finally:
                self.write_notices(host_name, con)

        self.write_fragment_ok_notice(host_name)

    def finish_host(self, hosts_descr, host):
        host_name = host['name']

        if self._execute:
            con = self._con_map[host_name]

            try:
                if self._pretend:
                    con.rollback()
                else:
                    con.commit()
            finally:
                self.write_notices(host_name, con)

        if self._notices:
            nfd = self._nfd_map[host_name]

            nfd.close()
            del self._nfd_map[host_name]

        if self._output is not None:
            fd = self._fd_map[host_name]

            self._sql_file_utils.write_footer(fd)

            fd.close()
            del self._fd_map[host_name]

        if self._execute:
            con = self._con_map[host_name]

            con.close()
            del self._con_map[host_name]

    def finish(self, hosts_descr, finish_host_verb_func=None):
        for host in hosts_descr.host_list:
            if finish_host_verb_func is not None:
                host_name = host['name']

                finish_host_verb_func(host_name)

            self.finish_host(hosts_descr, host)

    def close(self):
        for host_name, nfd in reversed(list(self._nfd_map.items())):
            nfd.close()
            del self._nfd_map[host_name]

        for host_name, fd in reversed(list(self._fd_map.items())):
            fd.close()
            del self._fd_map[host_name]

        for host_name, con in reversed(list(self._con_map.items())):
            con.close()
            del self._con_map[host_name]

# vi:ts=4:sw=4:et
