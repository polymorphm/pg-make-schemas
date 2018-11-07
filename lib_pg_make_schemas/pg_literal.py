def pg_quote(value):
    if value is None:
        return 'null'

    return 'e\'{}\''.format(
        str(value)
        .replace('\\', '\\\\')
        .replace('\'', '\\\'')
        .replace('\n', '\\n')
    )

def pg_ident_quote(ident):
    if ident is None:
         raise ValueError('ident is None')

    return '"{}"'.format(str(ident).replace('"', '""'))

def pg_dollar_quote(tag, value):
    if value is None:
        return 'null'

    i = -1

    while True:
        i += 1
        full_tag = '${}{}$'.format(tag, i if i > 0 else '')

        if full_tag not in value:
            return '{}{}{}'.format(full_tag, value, full_tag)

# vi:ts=4:sw=4:et
