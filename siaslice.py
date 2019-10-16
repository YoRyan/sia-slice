#!/usr/bin/env python3

import asyncio
import logging
import lzma
import os
import pickle
import re
from argparse import ArgumentParser
from collections import namedtuple
from datetime import datetime
from hashlib import md5
from io import BytesIO
from lzma import compress, LZMADecompressor
from types import AsyncGeneratorType, GeneratorType

from aioify import aioify
from defaultlist import defaultlist
import aiofile
import aiohttp


aiomd5 = aioify(obj=md5)
aiolzc = aioify(obj=compress)

DEFAULT_BLOCK_MB = 40
MAX_CONCURRENT_UPLOADS = 20
MAX_CONCURRENT_DOWNLOADS = 10
USER_AGENT = 'Sia-Agent'

BlockMap = namedtuple('BlockMap', ['block_size', 'md5_hashes'])
Block = namedtuple('Block', ['md5_hash', 'compressed_bytes'])
SiadEndpoint = namedtuple('SiadEndpoint', ['domain', 'api_password'])
OpStatus = namedtuple('OpStatus', ['transfers', 'current_index'])

class SiadError(Exception):
    def __init__(self, status, message):
        self.status = status
        self.message = message
    def __str__(self):
        return f'<Sia: [{self.status}] {self.message}>'
    def __repr__(self):
        return self.__str__()


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
            help='resume a stalled operation with the provided state file')
    argp.add_argument('file', help=('file target for uploads, source for '
                                    'downloads, or state to resume from'))
    argp.add_argument(
            'siapath', nargs='?',
            help='Sia directory target for uploads or source for downloads')
    args = argp.parse_args()

    endpoint = SiadEndpoint(domain='http://localhost:9980',
                            api_password=os.environ['SIAD_API'])
    async def siapath():
        if not args.siapath:
            raise ValueError('no siapath specified')
        siapath_valid = (await siad_post(
                endpoint, b'', 'renter', 'validate', args.siapath)).status == 204
        if not siapath_valid:
            raise ValueError(f'invalid siapath: {args.siapath}')
        return args.siapath.split('/')
    if args.mb:
        await do_mirror(endpoint, args.file, await siapath(), block_size=args.mb)
    elif args.download:
        await do_download(endpoint, args.file, await siapath())
    elif args.resume:
        with aiofile.AIOFile(args.file, 'rb') as state_afp:
            status_pickle = pickle.loads(await state_afp.read())
        if 'siaslice-mirror' in args.file:
            await do_mirror(
                    endpoint, status_pickle['source_file'], status_pickle['siapath'],
                    block_size=status_pickle['block_size'],
                    start_block=status_pickle['current_index'])
        elif 'siaslice-download' in args.file:
            pass
        else:
            raise ValueError(f'bad state file: {args.file}')


async def do_mirror(endpoint, source_file, siapath,
                    block_size=DEFAULT_BLOCK_MB*1e3*1e3, start_block=0):
    prior_map = await siapath_block_map(endpoint, siapath,
                                        fallback_block_size=block_size)
    if prior_map.block_size != block_size:
        raise ValueError(
                f'block size mismatch: expected {format_bs(block_size)}, '
                f'found {format_bs(prior_map.block_size)}')

    state_file = f"siaslice-mirror-{datetime.now().strftime('%Y%m%d-%H%M')}.dat"
    state_afp = aiofile.AIOFile(state_file, mode='wb')
    source_afp = aiofile.AIOFile(source_file, mode='rb')
    async for status in siapath_mirror(endpoint, source_afp, prior_map,
                                       start_block=start_block):
        await state_afp.write(pickle.dumps({
                'source_file': source_file,
                'siapath': siapath,
                'block_size': block_size,
                'current_index': status.current_index}))
    source_afp.close()
    state_afp.close()
    os.remove(state_file)


async def siapath_mirror(endpoint, source_afp, siapath, prior_map, start_block=0):
    uploads = {}
    current_index = start_block
    update = asyncio.Event()

    async def upload(index, block):
        nonlocal uploads, current_index, update
        up_siapath = siapath + (f'siaslice.{format_bs(prior_map.block_size)}.'
                                f'{index}.{block.md5_hash}.lz',)
        await siapath_delete_block(endpoint, siapath, index)
        await siad_post(endpoint, BytesIO(block.compressed_bytes),
                        'renter', 'uploadstream', *up_siapath)
        up_status = {}
        while not up_status.get('recoverable', False):
            up_status = (await siad_json(await siad_get(
                    endpoint, 'renter', 'file', *up_siapath))).get('file', {})
            uploads[index] = up_status.get('uploadprogress', 0)/100.0
            update.set()
            await asyncio.sleep(10)
        else:
            del uploads[index]
            update.set()

    async def read():
        nonlocal current_index
        async for index, block, change in read_blocks(
                source_afp, prior_map, start_block):
            current_index = index
            update.set()
            if change:
                yield (index, block)

    main_done = False
    async def main():
        nonlocal upload, read, main_done, update
        await run_all_tasks((upload(index, block) async for index, block in read()),
                            max_concurrent=MAX_CONCURRENT_UPLOADS)
        main_done = True
        update.set()

    main_task = asyncio.create_task(main())
    while not main_done:
        yield OpStatus(transfers=uploads, current_index=current_index)
        await update.wait()
        update.clear()
    else:
        await main_task


async def siapath_delete_block(endpoint, siapath, block_index):
    response = await siad_get(endpoint, 'renter', 'dir', *siapath)
    if response.status == 500: # nonexistent directory
        pass
    elif response.status == 200:
        paths = (meta['siapath']
                 for meta in (await siad_json(response)).get('files', []))
        for path in paths:
            match = re.search(
                    rf'siaslice\.\d+MiB\.{block_index}\.[a-z\d]+\.lz$', path)
            if match:
                await siad_post(endpoint, b'', 'renter', 'delete', *path.split('/'))
    else:
        raise ValueError(f"{'/'.join(siapath)} is a file, not a directory")


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


async def do_download(endpoint, target_file, siapath, start_block=0):
    state_file = f"siaslice-download-{datetime.now().strftime('%Y%m%d-%H%M')}.dat"
    state_afp = aiofile.AIOFile(state_file, mode='wb')
    target_afp = aiofile.AIOFile(target_file, mode='wb')
    async for status in siapath_download(endpoint, target_afp, siapath,
                                         start_block=start_block):
        await state_afp.write(pickle.dumps({
                'target_file': target_file,
                'siapath': siapath,
                'current_index': status.current_index}))
    target_afp.close()
    state_afp.close()
    os.remove(state_file)


async def siapath_download(endpoint, target_afp, siapath, start_block=0):
    downloads = {}
    current_index = start_block
    update = asyncio.Event()

    block_map = await siapath_block_map(endpoint, siapath)
    md5_hashes = block_map.md5_hashes[start_block:]

    async def download(index, block_siapath):
        nonlocal block_map, downloads, current_index
        info = (await siad_json(await siad_get(
                endpoint, 'renter', 'file', *block_siapath))).get('file', {})
        filesize = info.get('filesize', 0)
        written = 0
        async for chunk in siad_stream_lz(endpoint, *block_siapath):
            await target_afp.write(
                    chunk, offset=(index*block_map.block_size + written))
            written += len(chunk)

            if filesize > 0:
                downloads[index] = written/filesize
            else:
                downloads[index] = 0.0
            update.set()

        if index in downloads:
            del downloads[index]
        current_index = min(current_index, index)
        update.set()

    main_done = False
    async def main():
        nonlocal download, md5_hashes
        def block_siapath(index, md5_hash):
            return siapath + (f'siaslice.{format_bs(block_map.block_size)}'
                              f'MiB.{index}.{md5_hash}.lz',)
        await run_all_tasks(
                (download(index, block_siapath(index, md5_hash))
                 for index, md5_hash in enumerate(md5_hashes) if md5_hash),
                max_concurrent=MAX_CONCURRENT_DOWNLOADS)
        main_done = True
        update.set()

    main_task = asyncio.create_task(main())
    while not main_done:
        yield OpStatus(transfers=downloads, current_index=current_index)
        await update.wait()
        update.clear()
    else:
        await main_task


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
                                 f'{format_bs(siapath_block_size)}MiB')
            filename_index = int(match.group(2))
            filename_hash = match.group(3)

            # Duplicate block indices should never happen.
            if hashes[filename_index]:
                raise IndexError(f'duplicate block: {filename}')
            hashes[filename_index] = filename_hash

        return BlockMap(block_size=block_size, md5_hashes=hashes)
    else:
        raise ValueError(f"{'/'.join(siapath)} is a file, not a directory")


def format_bs(block_size):
    n = int(block_size/1e3/1e3)
    return f'{n}MiB'


async def siad_stream_lz(endpoint, *siapath):
    loop = asyncio.get_running_loop()
    response = await siad_get(endpoint, 'renter', 'stream', *siapath)
    alzd = aioify(obj=LZMADecompressor().decompress)
    while True:
        chunk = await response.content.read(1*1e3*1e3)
        if chunk:
            yield alzd(chunk)
        else:
            break


async def siad_get(endpoint, *path, **qs):
    url = f"{endpoint.domain}/{'/'.join(path)}"
    headers = { 'User-Agent': USER_AGENT }
    session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth('', password=endpoint.api_password),
            timeout=aiohttp.ClientTimeout(total=None))
    return await session.get(url, params=qs, headers=headers)


async def siad_post(endpoint, post_data, *path, **qs):
    url = f"{endpoint.domain}/{'/'.join(path)}"
    headers = { 'User-Agent': USER_AGENT }
    session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth('', password=endpoint.api_password),
            timeout=aiohttp.ClientTimeout(total=None))
    return await session.post(url, data=post_data, params=qs, headers=headers)


async def siad_json(response):
    json = await response.json()
    status = response.status
    if (status >= 400 and status <= 499) or (status >= 500 and status <= 599):
        raise SiadError(status, json.get('message'))
    return json


async def run_all_tasks(generator, max_concurrent=0):
    sem = asyncio.BoundedSemaphore(value=max_concurrent)
    running = 0
    complete = asyncio.Event()
    async def run_task(the_cor):
        nonlocal running, sem, complete
        await the_cor
        running -= 1
        sem.release()
        complete.set()
    if isinstance(generator, GeneratorType):
        for cor in generator:
            await sem.acquire()
            running += 1
            asyncio.create_task(run_task(cor))
    elif isinstance(generator, AsyncGeneratorType):
        async for cor in generator:
            await sem.acquire()
            running += 1
            asyncio.create_task(run_task(cor))
    else:
        raise ValueError(f'not a generator: {generator}')

    while running > 0:
        await complete.wait()
        complete.clear()


if __name__ == '__main__':
    main()

