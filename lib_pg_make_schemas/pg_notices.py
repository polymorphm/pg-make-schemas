class PgNotices:
    def __init__(self):
        self._notice_list = []

    # this function is interface for psycopg2-connection
    def append(self, notice):
        self._notice_list.append(notice)

    def pop_all(self):
        notices = self._notice_list.copy()
        self._notice_list.clear()
        return notices

# vi:ts=4:sw=4:et
