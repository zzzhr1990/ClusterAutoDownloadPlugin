# -*- coding: utf-8 -*-
#
# Copyright (C) 2007,2008 Andrew Resch <andrewresch@gmail.com>
#
# This file is part of Deluge and is licensed under GNU General Public License 3.0, or later, with
# the additional special exception to link portions of this program with the OpenSSL library.
# See LICENSE for more details.
#

"""Common functions for various parts of Deluge to use."""
from __future__ import division, print_function, unicode_literals

import base64
import functools
import locale
import logging
import numbers
import os
import platform
import re
import subprocess
import sys
import time

import pkg_resources

try:
    from urllib.parse import unquote_plus, urljoin
    from urllib.request import pathname2url
except ImportError:
    # PY2 fallback
    from urlparse import urljoin  # pylint: disable=ungrouped-imports
    from urllib import pathname2url, unquote_plus  # pylint: disable=ungrouped-imports


MAGNET_SCHEME = 'magnet:?'
XT_BTIH_PARAM = 'xt=urn:btih:'
DN_PARAM = 'dn='
TR_PARAM = 'tr='


def is_infohash(infohash):
    """
    A check to determine if a string is a valid infohash.
    Args:
        infohash (str): The string to check.
    Returns:
        bool: True if valid infohash, False otherwise.
    """
    return len(infohash) == 40 and infohash.isalnum()


def is_magnet(uri):
    """
    A check to determine if a uri is a valid bittorrent magnet uri
    :param uri: the uri to check
    :type uri: string
    :returns: True or False
    :rtype: bool
    :Example:
    >>> is_magnet('magnet:?xt=urn:btih:SU5225URMTUEQLDXQWRB2EQWN6KLTYKN')
    True
    """

    if uri.startswith(MAGNET_SCHEME) and XT_BTIH_PARAM in uri:
        return True
    return False


def get_magnet_info(uri):
    """Parse torrent information from magnet link.
    Args:
        uri (str): The magnet link.
    Returns:
        dict: Information about the magnet link.
        Format of the magnet dict::
            {
                "name": the torrent name,
                "info_hash": the torrents info_hash,
                "files_tree": empty value for magnet links
            }
    """

    tr0_param = 'tr.'
    tr0_param_regex = re.compile("^tr.(\\d+)=(\\S+)")
    if not uri.startswith(MAGNET_SCHEME):
        return {}

    name = None
    info_hash = None
    trackers = {}
    tier = 0
    for param in uri[len(MAGNET_SCHEME):].split('&'):
        if param.startswith(XT_BTIH_PARAM):
            xt_hash = param[len(XT_BTIH_PARAM):]
            if len(xt_hash) == 32:
                try:
                    info_hash = base64.b32decode(xt_hash.upper()).encode('hex')
                except TypeError as ex:
                    logging.debug(
                        'Invalid base32 magnet hash: %s, %s', xt_hash, ex)
                    break
            elif is_infohash(xt_hash):
                info_hash = xt_hash.lower()
            else:
                break
        elif param.startswith(DN_PARAM):
            name = unquote_plus(param[len(DN_PARAM):])
        elif param.startswith(TR_PARAM):
            tracker = unquote_plus(param[len(TR_PARAM):])
            trackers[tracker] = tier
            tier += 1
        elif param.startswith(tr0_param):
            try:
                tier, tracker = re.match(tr0_param_regex, param).groups()
                trackers[tracker] = tier
            except AttributeError:
                pass

    if info_hash:
        if not name:
            name = info_hash
        return {'name': name, 'info_hash': info_hash, 'files_tree': '', 'trackers': trackers}
    else:
        return {}


def create_magnet_uri(infohash, name=None, trackers=None):
    """Creates a magnet uri
    Args:
        infohash (str): The info-hash of the torrent.
        name (str, optional): The name of the torrent.
        trackers (list or dict, optional): A list of trackers or dict or {tracker: tier} pairs.
    Returns:
        str: A magnet uri string.
    """

    uri = [MAGNET_SCHEME, XT_BTIH_PARAM,
           base64.b32encode(infohash.decode('hex'))]
    if name:
        uri.extend(['&', DN_PARAM, name])
    if trackers:
        try:
            for tracker in sorted(trackers, key=trackers.__getitem__):
                uri.extend(['&', 'tr.%d=' % trackers[tracker], tracker])
        except TypeError:
            for tracker in trackers:
                uri.extend(['&', TR_PARAM, tracker])

    return ''.join(uri)
