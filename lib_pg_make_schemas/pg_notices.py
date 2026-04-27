class PgNotices:
    def __init__(self):
        self._notice_list = []

    # This is the notice-handler interface used by Psycopg 3 connections.
    def __call__(self, diagnostic):
        severity = diagnostic.severity
        message = diagnostic.message_primary

        if severity is None:
            notice_list = [message]
        else:
            notice_list = ['{}:  {}'.format(severity, message)]

        if diagnostic.message_detail is not None:
            notice_list.append('DETAIL:  {}'.format(diagnostic.message_detail))

        if diagnostic.message_hint is not None:
            notice_list.append('HINT:  {}'.format(diagnostic.message_hint))

        self._notice_list.append('\n'.join(notice_list))

    def pop_all(self):
        notices = self._notice_list.copy()
        self._notice_list.clear()
        return notices

# vi:ts=4:sw=4:et
