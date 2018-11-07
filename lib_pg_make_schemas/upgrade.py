class UpgradeError(Exception):
    pass

class AmbiguousUpgradeError(UpgradeError):
    pass

def print_revision(
            host_name,
            host_type,
            var_rev,
            var_com,
            func_rev,
            func_com,
            print_func,
        ):
    if var_rev == func_rev and var_com == func_com:
        print_func(
            '{!r} ({!r}) has revision {!r}{}'.format(
                host_name,
                host_type,
                var_rev,
                ' comment {!r}'.format(var_com)
                        if var_com is not None else '',
            ),
        )
    else:
        print_func(
            '{!r} ({!r}) has var revision {!r}{}'.format(
                host_name,
                host_type,
                var_rev,
                ' comment {!r}'.format(var_com)
                        if var_com is not None else '',
            ),
        )
        print_func(
            '{!r} ({!r}) has func revision {!r}{}'.format(
                host_name,
                host_type,
                func_rev,
                ' comment {!r}'.format(func_com)
                        if func_com is not None else '',
            ),
        )

def print_migr_way(host_name, host_type, migr_list, print_func):
    if migr_list is None:
        print_func(
            '{!r} ({!r}) has no a migration way'.format(
                host_name,
                host_type,
            ),
        )

        return

    if not migr_list:
        print_func(
            '{!r} ({!r}) needs no migration'.format(
                host_name,
                host_type,
            ),
        )

        return

    migr_way = ', '.join(
        '{!r} from {!r}'.format(migr[0], migr[1])
                for migr in migr_list
    )

    print_func(
        '{!r} ({!r}) has the migration way: {}'.format(
            host_name,
            host_type,
            migr_way,
        ),
    )

def find_migr_way(
                cluster_descr,
                host_type,
                var_rev,
            ):
    target_rev = cluster_descr.revision

    if var_rev == target_rev:
        return []

    migrations_descr = cluster_descr.migrations

    if migrations_descr is None:
        return

    comp_list_map = {}
    migr_list_candidates = []

    for migration_descr in migrations_descr.migration_list:
        comp_list = comp_list_map.setdefault(migration_descr.revision, [])
        comp_list.extend(migration_descr.compatible_list)

        if migration_descr.revision != target_rev:
            continue

        for comp_rev in migration_descr.compatible_list:
            migr_list_candidates.append([(target_rev, comp_rev)])

    result_migr_list = None

    while migr_list_candidates:
        for migr_list in migr_list_candidates:
            top_from_rev = migr_list[0][1]

            if top_from_rev != var_rev:
                continue

            if result_migr_list is not None:
                raise AmbiguousUpgradeError(
                    '{!r}, {!r}: ambiguous migration way'.format(
                        result_migr_list,
                        migr_list,
                    ),
                )

            result_migr_list = migr_list

        if result_migr_list is not None:
            return result_migr_list

        next = []

        for migr_list in migr_list_candidates:
            top_from_rev = migr_list[0][1]

            comp_list = comp_list_map.get(top_from_rev)

            if comp_list is None:
                continue

            for comp_rev in comp_list:
                if (top_from_rev, comp_rev) in comp_list:
                    continue

                next.append([(top_from_rev, comp_rev)] + migr_list)

        migr_list_candidates = next

# vi:ts=4:sw=4:et
