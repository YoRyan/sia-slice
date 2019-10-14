#!/usr/bin/env python3

import asyncio
import logging
import lzma
import os
import re
from argparse import ArgumentParser
from collections import namedtuple
from datetime import datetime
from hashlib import md5
from io import BytesIO

from aioify import aioify
from defaultlist import defaultlist
import aiofile
import aiohttp


aiomd5 = aioify(obj=md5)
aiolzc = aioify(obj=lzma.compress)
aiolzd = aioify(obj=lzma.decompress)

DEFAULT_BLOCK_MB = 40
MAX_CONCURRENT_UPLOADS = 10
USER_AGENT = 'Sia-Agent'

BlockMap = namedtuple('BlockMap', ['block_size', 'md5_hashes'])
Block = namedtuple('Block', ['md5_hash', 'compressed_bytes'])
SiadEndpoint = namedtuple('SiadEndpoint', ['domain', 'api_password'])

class SiadError(Exception):
    def __init__(self, status, message):
        super().__init__(self)
        self.status = status
        self.message = message
    def __str__(self):
        return f'<[{status}] {message}>'


def main(*arg, **kwarg):
    asyncio.run(amain(*arg, **kwarg))

async def amain():
    argp = ArgumentParser(
            description='Sync a large file to Sia with incremental updates.')
    argp_op = argp.add_mutually_exclusive_group(required=True)
    argp_op.add_argument(
            '-m', '--mirror', dest='mb', const='40', type=int, nargs='?',
            help=('sync a copy to Sia by dividing the file into chunks '
                  f'of MB megabytes (default {DEFAULT_BLOCK_MB}MiB; cannot be '
                  'changed after the initial upload)'))
    argp_op.add_argument('-d', '--download', action='store_true',
                         help='reconstruct a copy using Sia')
    argp_op.add_argument(
            '-r', '--resume', action='store_true',
            help='resume a stalled operation with the provided log file')
    argp.add_argument('file', help=('file target for uploads, source for '
                                    'downloads, or log to resume from'))
    argp.add_argument(
            'siapath', nargs='?',
            help='Sia directory target for uploads or source for downloads')
    args = argp.parse_args()

    endpoint = SiadEndpoint(domain='http://localhost:9980',
                            api_password=os.environ['SIAD_API'])
    if args.mb:
        if not args.siapath:
            raise ValueError('no siapath specified')
        siapath_valid = (await siad_post(
                endpoint, b'', 'renter', 'validate', args.siapath)).status == 204
        if not siapath_valid:
            raise ValueError(f'invalid siapath: {args.siapath}')

        siapath = args.siapath.split('/')
        async with aiofile.AIOFile(args.file, mode='rb') as source_afp:
            await do_sia_sync(endpoint, source_afp, siapath, args.mb*1e3*1e3)
    elif args.download:
        pass
    elif args.resume:
        # (Currently we don't do anything with the log file except locate the
        #  last successfully transferred block.)
        with aiofile.AIOFile(args.file, 'rt') as log_afp:
            saved_block_map = await read_map_file(aiofile.Reader(log_afp))
            start_block = len(saved_block_map.md5_hashes)


async def read_map_file(afp):
    block_line = await afp.readline()
    block_match = re.search(r'^([0-9]+)MiB$', block_line)
    if not block_match:
        raise ValueError(f'invalid block size: {block_line}')
    block_size = int(block_match.group(1))*1e3*1e3
    md5_hashes = [line async for line in afp]
    return BlockMap(block_size=block_size, md5_hashes=md5_hashes)


async def do_sia_sync(endpoint, source_afp, siapath, block_size, start_block=0):
    prior_map = await siapath_block_map(endpoint, siapath,
                                        fallback_block_size=block_size)
    if prior_map.block_size != block_size:
        logging.warning(
                f'block size mismatch - expected {format_bs(block_size)}, found '
                f'{format_bs(prior_map.block_size)}; using the detected size')
        block_size = prior_map.block_size

    read_done = False
    uploads = []
    uploads_sem = asyncio.BoundedSemaphore(value=MAX_CONCURRENT_UPLOADS)
    async def check_progress_worker():
        while not read_done or uploads != []:
            # Check upload status and mark complete uploads.
            async def is_done(upload):
                status = await siad_json(await siad_get(
                        endpoint, 'renter', 'file', *upload))
                return status.get('uploadprogress', 0) >= 100
            to_remove = [upload for upload in uploads.copy()
                         if await is_done(upload)]
            for upload in to_remove:
                uploads.remove(upload)
                uploads_sem.release()
            await asyncio.sleep(30)
    check_progress_task = asyncio.create_task(check_progress_worker())

    timestamp = datetime.now().strftime('%Y%m%d-%H%M')
    async with aiofile.AIOFile(f'siaslice-{timestamp}.log', 'wt') as log_afp:
        log_write = aiofile.Writer(log_afp)

        # Write log file header and already-processed blocks.
        await log_write(f'{format_bs(block_size)}\n')
        for index, md5_hash in enumerate(prior_map.md5_hashes):
            if index >= start_block:
                break
            else:
                await log_write(f'{md5_hash}\n')

        # Read and upload blocks.
        async for index, block, change in read_blocks(source_afp, prior_map,
                                                      start_block):
            if change:
                filename = ('siaslice.'
                            f'{format_bs(block_size)}.{index}.{block.md5_hash}.lz')
                block_siapath = siapath + (filename,)
                async with uploads_sem:
                    await siapath_delete_block(endpoint, siapath, index)
                    await siad_post(endpoint, BytesIO(block.compressed_bytes),
                                    'renter', 'uploadstream', *block_siapath)
                    uploads.append(block_siapath)
            await log_write(f'{block.md5_hash}\n')

    # Clean up.
    read_done = True
    await check_progress_task
    os.remove(log_file)


def format_bs(block_size):
    n = int(block_size/1e3/1e3)
    return f'{n}MiB'


async def siapath_block_map(
        endpoint, siapath, fallback_block_size=DEFAULT_BLOCK_MB*1e3*1e3):
    response = await siad_get(endpoint, 'renter', 'dir', *siapath)
    if response.status == 500: # nonexistent directory
        return BlockMap(block_size=fallback_block_size, md5_hashes=[])
    elif response.status == 200:
        filenames = (meta['siapath'].split('/')[-1:][0]
                     for meta in (await siad_json(response)).get('files', []))
        block_size = None
        hashes = defaultlist(lambda: '')
        for filename in filenames:
            # Extract block size, index, and MD5 hash from filename.
            match = re.search(r'^siaslice\.(\d+)MiB\.(\d+)\.([a-z\d]+)\.lz$',
                              filename)
            if not match:
                continue
            filename_block_size = int(match.group(1))*1e3*1e3
            if not block_size:
                if filename_block_size <= 0:
                    raise ValueError(f'invalid block size: {filename}')
                block_size = filename_block_size
            elif block_size != filename_block_size:
                raise ValueError(f'inconsistent block size: {filename} vs. '
                                 f'{siapath_block_size}MiB')
            filename_index = int(match.group(2))
            filename_hash = match.group(3)

            # Duplicate block indices should never happen.
            if hashes[filename_index]:
                raise IndexError(f'duplicate block: {filename}')
            hashes[filename_index] = filename_hash

        return BlockMap(block_size=block_size, md5_hashes=hashes)
    else:
        raise ValueError(f"{'/'.join(siapath)} is a file, not a directory")


async def siapath_delete_block(endpoint, siapath, block_index):
    response = await siad_get(endpoint, 'renter', 'dir', *siapath)
    if response.status == 500: # nonexistent directory
        pass
    elif response.status == 200:
        paths = (meta['siapath']
                 for meta in (await siad_json(response)).get('files', []))
        for path in paths:
            match = re.search(rf'siaslice\.\d+MiB\.{block_index}\.[a-z\d]+\.lz$',
                              path)
            if match:
                await siad_post(endpoint, b'', 'renter', 'delete', *path.split('/'))
    else:
        raise ValueError(f"{'/'.join(siapath)} is a file, not a directory")


async def siad_get(endpoint, *path, **qs):
    url = f"{endpoint.domain}/{'/'.join(path)}"
    headers = { 'User-Agent': USER_AGENT }
    async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth('', password=endpoint.api_password),
            timeout=aiohttp.ClientTimeout(total=None)) as session:
        return await session.get(url, params=qs, headers=headers)


async def siad_post(endpoint, post_data, *path, **qs):
    url = f"{endpoint.domain}/{'/'.join(path)}"
    headers = { 'User-Agent': USER_AGENT }
    async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth('', password=endpoint.api_password),
            timeout=aiohttp.ClientTimeout(total=None)) as session:
        return await session.post(url, data=post_data, params=qs, headers=headers)


async def siad_json(response):
    json = await response.json()
    status = response.status
    if (status >= 400 and status <= 499) or (status >= 500 and status <= 599):
        raise SiadError(status, json.get('message'))
    return json


async def read_blocks(source_afp, prior_block_map, start_block):
    block_size = prior_block_map.block_size
    index = start_block
    reader = aiofile.Reader(
            source_afp, chunk_size=block_size, offset=start_block*block_size)
    async for chunk in reader:
        md5_hash, lz_bytes = await asyncio.gather(aiomd5(chunk), aiolzc(chunk))
        block = Block(md5_hash=md5_hash.hexdigest(), compressed_bytes=lz_bytes)
        try:
            prior_hash = prior_block_map.md5_hashes[index]
        except IndexError:
            prior_hash = ''
        block_changed = prior_hash != block.md5_hash
        yield (index, block, block_changed)
        index += 1


if __name__ == '__main__':
    main()

