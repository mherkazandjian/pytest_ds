#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Program entry point"""
from __future__ import print_function
import os
from os.path import abspath, expanduser, join, isdir
import sys

import argparse
from argparse import RawTextHelpFormatter

import pytest_ds
from pytest_ds import metadata, logger
from pytest_ds.tree import Query


DEFAULT_CONFIG_DIR = '~/.config/pytest_ds'
DEFAULT_CONFIG_FILE = 'config.ini'

cli_help = """

# the default configuration file is located at

      ~/.config/pytest_ds/config.ini

usage:

    # sync content as described in the default config file
    ~> pytest_ds_cli 

    # sync content as described in the specified config file
    ~> pytest_ds_cli --config=/path/to/my/config.ini

"""


def main(argv):
    """Program entry point.

    .. todo:: wrap the help strings to 80 chrs 

    :param argv: command-line arguments
    :type argv: :class:`list`
    """
    author_strings = []
    for name, email in zip(metadata.authors, metadata.emails):
        author_strings.append('Author: {0} <{1}>'.format(name, email))

    epilog = '''
{project} {version}

{description}
{authors}
URL: <{url}>
'''.format(
        project=metadata.project,
        description=metadata.description,
        version=metadata.version,
        authors='\n'.join(author_strings),
        url=metadata.url)

    parser = argparse.ArgumentParser(prog=argv[0],
                                     description=cli_help,
                                     formatter_class=RawTextHelpFormatter,
                                     epilog=epilog)

    parser.add_argument('-V', '--version',
                        action='version',
                        version='{0} {1}'.format(metadata.project,
                                                 metadata.version))

    parser.add_argument('-c', '--config',
                        type=str,
                        default=None,
                        help=(
                            "path to be synchronized, a relative or an "
                            "abspath. This path is used relative to src-base.")
                        )

    parser.add_argument("-n", "--dry",
                        action="count",
                        default=0,
                        help=(
                        "[off by default] when specified the files are listed ")
                        )

    parser.add_argument("--debug", "-dbg",
                        action="count",
                        default=0,
                        help="if used and an exception is raised a post-mortum "
                             "pdb session is started.")

    print(epilog)

    parsed_args = parser.parse_args(args=argv[1:])

    try:
        _main(parsed_args)
    except Exception as e:
        if parsed_args.debug > 0:
            print(e)
            import pdb
            pdb.post_mortem()
        else:
            logger.info('exception raised')
            logger.info('\t{}'.format(repr(e)))
            logger.info('exitting gracefully.')
        retval = 1
    else:
        retval = 0

    return retval


def _main(args):

    logger.info('using pytest_ds')
    logger.info('\t\t{}'.format(pytest_ds.__file__))

    config_path = find_config_file(args.config)

    syncer = Query(config=config_path, index_webdav_enabled=True)

    if args.dry == 0:
        syncer.sync(dry=False)
    elif args.dry == 1:
        syncer.ls_url()
        syncer.sync(dry=True)

        logger.info(
            'new items to be downloaded = {}'.format(
                len(syncer.summary['new']))
        )
        logger.info(
            'modified items to be download = {}'.format(
                len(syncer.summary['modified']))
        )
    else:
        msg = '--dry is specified more than once. unknown behavior.'
        raise ValueError(msg)


def find_config_file(config_file):
    """
    given a name of a configuration it is checked at the specified path, if 
    it is not found it is looked up in the config dir otherwise an os error is
    raised.
    """

    # config file is in the currecnt working directory or at the specified path
    config_path = expanduser(config_file)
    if os.path.isfile(config_path):
        return config_path

    # config file is in the default config directory
    path_in_config_dir = expanduser(join(DEFAULT_CONFIG_DIR,
                                         config_file))
    if os.path.isfile(path_in_config_dir):
        return path_in_config_dir

    msg = ('configuration file {} not found either in ath specified path '
           'nor in the default configuration dir'.format(config_file))
    raise ValueError(msg)


def entry_point():
    """Zero-argument entry point for use with setuptools/distribute."""
    raise SystemExit(main(sys.argv))


if __name__ == '__main__':
    entry_point()
