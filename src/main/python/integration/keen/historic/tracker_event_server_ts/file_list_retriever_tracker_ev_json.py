__author__ = 'broy'

import logging

from integration.keen.historic.common.file_list_retriever import FileListRetriever

from subprocess import Popen, PIPE

class FileListRetrieverTrackerEventJson(FileListRetriever):

    def get_file_names_in_dthr(self, dt, hr):
        logging.info("Inside get_partitions, dt={}, hr={}".format(dt, hr))

        dir_name = self._construct_full_path_name(dt, hr)
        proc = Popen(['hdfs', 'dfs', '-ls', dir_name], stdout=PIPE)
        output = proc.communicate()

        logging.info("hdfs dfs -ls ={}".format(output))
        files = [dir[dir.find(dir_name):] for dir in output[0].split("\n")[1:-1]]
        files = [file.replace(dir_name, "") for file in files]
        files = filter(lambda file: not file.startswith('.hive-staging'), files)
        logging.info("files={}".format(files))
        return files

    def _construct_full_path_name(self, dt, hr):
        return self._entity_base_dir_template.format(dt, hr)

    def construct_full_file_name(self, dt, hr, filename):
        return self._entity_filepath_template.format(dt, hr, filename)

    def __init__(self, entity_base_dir_template, entity_filepath_template):
        super(FileListRetrieverTrackerEventJson, self)\
            .__init__(entity_base_dir_template, entity_filepath_template)
