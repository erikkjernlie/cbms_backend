#!/usr/bin/env python3
"""The start point of the application."""
import os


os.environ['PATH'] = os.path.dirname(os.path.realpath(__file__)) + "\\src\\configuration\\" + os.pathsep + os.environ['PATH']

import argparse
import importlib
import logging
import os
from aiohttp import web

from src.server import init_app


class Settings:
    """A class for holding the application settings"""
    def __init__(self, settings_module):
        """
        Sets attributes to be all uppercase attributes from module

        :param settings_module: the module to import setting attributes from
        """
        for setting in dir(settings_module):
            if setting.isupper():
                setattr(self, setting, getattr(settings_module, setting))
        # Prevent any changes to the attributes after initialization
        self.__isfrozen = True


def main(args):
    """
    Start the application.

    Will be called with command line args if the file is run as a script
    """
    # Import settings
    settings_module = importlib.import_module(args.settings)
    settings = Settings(settings_module)
    # Setup logging
    logging.basicConfig(filename=settings.LOG_FILE, level=settings.LOG_LEVEL)
    logging.info('Starting app')
    # Start application
    web.run_app(app=init_app(settings),
                host=settings.HOST, port=settings.PORT)


if __name__ == '__main__':
    # Define command line interface

    parser = argparse.ArgumentParser(description='Run server')
    parser.add_argument('settings',
                        help='The module to import settings attributes from',
                        nargs='?', default='settings')
    main(parser.parse_args())
