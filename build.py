#import os
#import sys
from pybuilder.core import init
from pybuilder.core import use_plugin
from pybuilder.core import task

use_plugin("exec")
use_plugin("python.core")
use_plugin("python.unittest")
#use_plugin("python.coverage")
use_plugin("python.install_dependencies")
use_plugin("python.distutils")
use_plugin("source_distribution")

default_task = "publish"
name = 'r1-dw-mobile-lowlatency-app'
version = '1.0'

@task
def say_hello ():
    print "Hello, PyBuilder"

@init
def initialize(project):
    project.set_property('source_dist_ignore_patterns','*.pyc')
    project.set_property('dir_source_main_python','src/main/python')
    project.set_property('dir_source_unittest_python','src/unittest/python')
    #project.set_property('coverage_break_build', False)
    project.set_property('unittest_module_glob', 'test_*')
    project.depends_on('MySQL-python')
    #project.depends_on('mysqlclient')
    project.depends_on('pyhs2')
    project.depends_on('requests')
