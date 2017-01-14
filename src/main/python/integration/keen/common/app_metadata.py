__author__ = 'broy'

import logging
import traceback

import MySQLdb

from integration.keen.common import config


class AppMetadata:

    def get_app_metadata(self):

        logging.info("\nget_app_metadata invoked...")

        aws_db_connect = MySQLdb.connect(config.app_db_connect_host,
                                         config.app_db_connect_user,
                                         config.app_db_connect_password,
                                         config.app_db_connect_db)

        aws_db_connect.autocommit(False)
        cursor = aws_db_connect.cursor()

        app_metadata = []

        try:
            select_app_metadata = """
                select
                    app.keen_project_id,
                    app.app_id,
                    app.keen_custom_key,
                    app.id
                from connect_applications app
                """
            cursor.execute(select_app_metadata)

            app_metadata = [row for row in cursor]

            logging.debug("\n\n\napp_metadata = {}".format(app_metadata))

        except:
            logging.error('exception encountered when selecting app metadata...')
            logging.error(traceback.format_exc())
            raise

        logging.info("\nget_app_metadata invocation completed...")

        return app_metadata

    def get_app_metadata_dict(self):
        app_md_flat = self.get_app_metadata()
        return {app_md_row[1]: app_md_row for app_md_row in app_md_flat}
