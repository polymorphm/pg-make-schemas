# -*- mode: python; coding: utf-8 -*-

def read_upgrade_sql(cluster_descr, host_type, migr):
    migrations_descr = cluster_descr.migrations
    migration_descr = None
    
    if migrations_descr is not None:
        for other_migration_descr in migrations_descr.migration_list:
            if other_migration_descr.revision != migr[0] or \
                    migr[1] not in other_migration_descr.compatible_list:
                continue
            
            if migration_descr is not None:
                raise ValueError(
                    '{!r}, {!r}: non unique migration'.format(
                        cluster_descr.cluster_file_path,
                        migr,
                    ),
                )
            
            migration_descr = other_migration_descr
    
    if migration_descr is None:
        raise ValueError(
            '{!r}, {!r}: missing migration'.format(
                cluster_descr.cluster_file_path,
                migr,
            ),
        )
    
    for upgrade_descr in migration_descr.upgrade_list:
        if upgrade_descr.upgrade_type != host_type:
            continue
        
        yield from upgrade_descr.read_sql()
