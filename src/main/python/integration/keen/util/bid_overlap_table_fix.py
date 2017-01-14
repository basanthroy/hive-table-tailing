__author__ = 'broy'

import time

from subprocess import Popen, PIPE

class BidOverlapTableFix:

    __source_base_dir_content_cats = '/data/radiumone/connect/overlap/top_content_categories/'
    #__destination_base_dir_content_cats = '/user/hive/warehouse/broy.db/connect_overlap_categories/'
    __destination_base_dir_content_cats = '/data/radiumone/connect/overlap/connect_overlap_categories/'

    __source_base_dir_cities = '/data/radiumone/connect/overlap/top_cities/'
    #__destination_base_dir_cities = '/user/hive/warehouse/broy.db/connect_overlap_cities/'
    __destination_base_dir_cities = '/data/radiumone/connect/overlap/connect_overlap_cities/'

    __source_base_dir_domains = '/data/radiumone/connect/overlap/top_domains/'
    #__destination_base_dir_domains = '/user/hive/warehouse/broy.db/connect_overlap_domains/'
    __destination_base_dir_domains = '/data/radiumone/connect/overlap/connect_overlap_domains/'

    __source_base_dir_media_types = '/data/radiumone/connect/overlap/top_media_types/'
    #__destination_base_dir_media_types = '/user/hive/warehouse/broy.db/connect_media_types/'
    __destination_base_dir_media_types = '/data/radiumone/connect/overlap/connect_media_types/'

    __source_dest = {
        __source_base_dir_content_cats:__destination_base_dir_content_cats,
        __source_base_dir_cities:__destination_base_dir_cities,
        __source_base_dir_domains:__destination_base_dir_domains,
        __source_base_dir_media_types:__destination_base_dir_media_types
    }

    def get_partitions(self, source):
        proc = Popen(['hdfs', 'dfs', '-ls', source], stdout=PIPE)
        output = proc.communicate()
        partitions_list = [part.split(' ')[- 1].split('/')[-1] for part in output[0].split('\n')]
        partitions_list = [part for part in partitions_list if part <> 'items' and part <> '' and 'dt=' not in part]

        print "source = {}, partitions_list={}".format(source, partitions_list)
        return partitions_list

    def copy_files(self, source, destination, partitions_list):

        proc = Popen(['hdfs', 'dfs', '-mkdir', destination], stdout=PIPE)
        print "mkdir root dir, proc.comm = {}".format(proc.communicate())

        for partition in partitions_list:
            part_source = source + partition + '/os_n/*'
            part_destination = destination + 'dt=' + partition + '/'
            print "part_source={}".format(part_source)
            print "part_destination={}".format(part_destination)
            proc = Popen(['hdfs', 'dfs', '-mkdir', part_destination], stdout=PIPE)
            print "mkdir sub dir, proc.comm = {}".format(proc.communicate())
            proc = Popen(['hdfs', 'dfs', '-cp', part_source, part_destination], stdout=PIPE)
            # mkdir -p ?
            print "copy file, proc.comm = {}".format(proc.communicate())

    def process_tables(self):
        for src,dest in self.__source_dest.iteritems():
            print "process_tables, source={}, dest={}".format(src, dest)
            partitions = self.get_partitions(src)
            self.copy_files(src, dest, partitions)

if __name__ == "__main__":
    tick = time.time()

    print 'Starting table data refactoring, time={}, formatted time = {}'.format(str(tick), (
        time.strftime("%Y %m %e %H %M %S", time.localtime(tick))))

    print "\n\n### processing initiated..."
    fix_script = BidOverlapTableFix()
    fix_script.process_tables()
    print "\n\n### processing complete"

    print "elapsed time = {}".format(time.time() - tick)
    print 'Starting table data refactoring, time={}, formatted time = {}'.format(str(tick), (
        time.strftime("%Y %m %e %H %M %S", time.localtime(tick))))



