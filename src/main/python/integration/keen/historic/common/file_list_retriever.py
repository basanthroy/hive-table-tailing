__author__ = 'broy'


class FileListRetriever(object):
    def get_file_names_in_dthr(self, dt, hr):
        return []

    def _construct_full_path_name(self, dt, hr):
        return ""

    def construct_full_file_name(self, dt, hr, filename):
        return ""

    def __init__(self, entity_base_dir_template, entity_filepath_template):
        self._entity_base_dir_template = entity_base_dir_template
        self._entity_filepath_template = entity_filepath_template
