# =================================================================
#
# Terms and Conditions of Use
#
# Unless otherwise noted, computer program source code of this
# distribution # is covered under Crown Copyright, Government of
# Canada, and is distributed under the MIT License.
#
# The Canada wordmark and related graphics associated with this
# distribution are protected under trademark law and copyright law.
# No permission is granted to use them outside the parameters of
# the Government of Canada's corporate identity program. For
# more information, see
# http://www.tbs-sct.gc.ca/fip-pcim/index-eng.asp
#
# Copyright title to all 3rd party software distributed with this
# software is held by the respective copyright holders as noted in
# those files. Users are asked to read the 3rd Party Licenses
# referenced with those assets.
#
# Copyright (c) 2025 Government of Canada
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from datetime import datetime
import logging
import os
import tarfile
import zipfile

import ftputil
import rarfile

from woudc_data_registry import config

LOGGER = logging.getLogger(__name__)


def gather(px_basedir, skip_dirs):
    """gather candidate files from incoming FTP"""

    gathered = []
    gathered_files_to_remove = []

    host = config.WDR_FTP_HOST
    username = config.WDR_FTP_USER
    password = config.WDR_FTP_PASS
    basedir_incoming = config.WDR_FTP_BASEDIR_INCOMING
    keep_files = config.WDR_FTP_KEEP_FILES

    with ftputil.FTPHost(host, username, password) as ftp:
        LOGGER.info(f'Connected to {host}')
        LOGGER.info(f'Skip directories: {skip_dirs}')
        for root, dirs, files in ftp.walk(basedir_incoming, topdown=True):
            LOGGER.debug(f'Removing skip directories: {skip_dirs}')
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            for name in files:
                ipath = f'{root}/{name}'
                mtime = datetime.fromtimestamp(ftp.stat(ipath)[8])
                fpath = f'{px_basedir}/{ipath}'
                ipath_ftp_abs = f'{host}{ipath}'

                LOGGER.info(f'Adding file %s to processing queue: {ipath_ftp_abs}')  # noqa
                gathered.append({
                    'contributor': ipath.lstrip('/').split('/')[0],
                    'ipath': ipath,
                    'ipath_ftp_abs': ipath_ftp_abs,
                    'mtime': mtime,
                    'fpath': os.path.normpath(fpath),
                    'decomp': False
                })

        LOGGER.info(f'{len(gathered)} files in processing queue')
        for gathered_file in gathered:
            LOGGER.info(f'Inspecting gathered file {gathered_file}')
            if all([gathered_file['ipath'] is not None,
                    not gathered_file['decomp']]):
                LOGGER.info('File passed in')
                if ftp.path.isfile(gathered_file['ipath']):
                    try:
                        os.makedirs(os.path.dirname(gathered_file['fpath']))
                    except OSError as err:
                        msg = f"Local directory %s not created: {gathered_file['fpath']}: {err}"  # noqa
                        LOGGER.warning(msg)

                    ftp.download(gathered_file['ipath'],
                                 gathered_file['fpath'])

                    msg = f"Downloaded FTP file {gathered_file['ipath_ftp_abs']} to {gathered_file['fpath']}"  # noqa
                    LOGGER.info(msg)

                    # handle compressed files here
                    if any([tarfile.is_tarfile(gathered_file['fpath']),
                            rarfile.is_rarfile(gathered_file['fpath']),
                            zipfile.is_zipfile(gathered_file['fpath'])]):
                        LOGGER.info(f"Decompressing file: {gathered_file['fpath']}")  # noqa
                        comm_ipath_ftp_abs = gathered_file['ipath_ftp_abs']
                        comm_fpath = gathered_file['fpath']
                        comm_mtime = gathered_file['mtime']
                        comm_dirname = os.path.dirname(comm_fpath)

                        # decompress
                        try:
                            files = decompress(gathered_file['fpath'])
                        except Exception as err:
                            LOGGER.error('Unable to decompress %s: %s',
                                         gathered_file['fpath'], err)
                            continue
                        LOGGER.info(f"Decompressed files: {', '.join(files)}")

                        for fil in files:
                            fpath = os.path.join(comm_dirname, fil)
                            if os.path.isfile(fpath):
                                gathered.append({
                                    'contributor': gathered_file[
                                        'ipath'].lstrip('/').split('/')[0],
                                    'ipath': fpath,
                                    'ipath_ftp_abs': comm_ipath_ftp_abs,
                                    'mtime': comm_mtime,
                                    'fpath': os.path.normpath(fpath),
                                    'decomp': True
                                })
                        # remove from gathered
                        gathered_files_to_remove.append(gathered_file)

                    if not keep_files:
                        LOGGER.info(f"Removing file {gathered_file['ipath']}")
                        ftp.remove(gathered_file['ipath'])

                else:
                    msg = f"FTP file {gathered_file['ipath_ftp_abs']} could not be downloaded"  # noqa
                    LOGGER.info(msg)

    msg = f'{len(gathered_files_to_remove)} archive files gathered to remove'
    LOGGER.info(msg)

    for gftr in gathered_files_to_remove:
        gathered.remove(gftr)
        LOGGER.info(f'Removed {gftr} from gathered list')

    LOGGER.info(f'{len(gathered)} files gathered')

    return gathered


def decompress(ipath):
    """decompress compressed files"""

    file_list = []
    success = True

    LOGGER.debug(f'ipath: {ipath}')

    if tarfile.is_tarfile(ipath):
        tar = tarfile.open(ipath)
        for item in tar:
            try:
                item.name = os.path.basename(item.name)
                file_list.append(item.name)
                tar.extract(item, os.path.dirname(ipath))
            except Exception as err:
                success = False
                msg = f'Unable to decompress from tar {item.name}: {err}'
                LOGGER.error(msg)

    elif rarfile.is_rarfile(ipath):
        rar = rarfile.RarFile(ipath)
        for item in rar.infolist():
            try:
                item.filename = os.path.basename(item.filename)
                file_list.append(item.filename)
                rar.extract(item, os.path.dirname(ipath))
            except Exception as err:
                success = False
                msg = f'Unable to decompress from rar {item.filename}: {err}'
                LOGGER.error(msg)

    elif zipfile.is_zipfile(ipath):
        zipf = zipfile.ZipFile(ipath)
        for item in zipf.infolist():
            if item.filename.endswith('/'):  # filename is dir, skip
                LOGGER.info('item %s is a directory, skipping', item.filename)
                continue
            try:
                item_filename = os.path.basename(item.filename)
                file_list.append(item_filename)
                data = zipf.read(item.filename)
                filename = f'{os.path.dirname(ipath)}/{item_filename}'
                LOGGER.info(f'Filename: {filename}')
                with open(filename, 'wb') as ff:
                    ff.write(data)
                # zipf.extract(item.filename, os.path.dirname(ipath))
            except Exception as err:
                success = False
                msg = f'Unable to decompress from zip {item.filename}: {err}'
                LOGGER.error(msg)

    else:
        msg = f'File {ipath} is not a compressed file and will not be decompressed'  # noqa
        LOGGER.warning(msg)

    if success:  # delete the archive file
        LOGGER.info(f'Deleting archive file: {ipath}')
        try:
            os.unlink(ipath)
        except Exception as err:
            LOGGER.error(err)
    else:
        LOGGER.info(f'Decompress failed, not deleting {ipath}')

    return file_list
