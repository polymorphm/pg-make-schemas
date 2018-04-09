# -*- mode: python; coding: utf-8 -*-

def check_settings_compatibility(
            source_code_cluster_descr,
            settings_cluster_descr,
        ):
    if settings_cluster_descr.application != source_code_cluster_descr.application:
        raise ValueError(
            '{!r}: settings_cluster_descr.application != source_code_cluster_descr.application'.format(
                settings_cluster_descr.cluster_file_path
            ),
        )
    
    if source_code_cluster_descr.revision not in settings_cluster_descr.compatible_list:
        raise ValueError(
            '{!r}: source_code_cluster_descr.revision not in settings_cluster_descr.compatible_list'.format(
                settings_cluster_descr.cluster_file_path
            ),
        )
