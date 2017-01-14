__author__ = 'broy'

from multiprocessing import Process, Pipe


class ProcessChunked(Process):
    app_metadata_row = ()
    start_date = -1
    end_date = -1

    def __str__(self):
        st = "Process = {}, start_date = {}, end_date = {}, app_metadata_row = {}"\
            .format(super.__str__(self), self.start_date, self.end_date, str(self.app_metadata_row[1]))
        return st
