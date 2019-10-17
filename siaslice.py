#!/usr/bin/env python3

import asyncio
import curses
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

import aiofile
import aiohttp
from aioify import aioify
from defaultlist import defaultlist


aiomd5 = aioify(obj=md5)
aiolzc = aioify(obj=compress)

BLOCK_MB = 100
MAX_CONCURRENT_UPLOADS = 20
MAX_CONCURRENT_DOWNLOADS = 10
USER_AGENT = 'Sia-Agent'

BlockMap = namedtuple('BlockMap', ['block_size', 'md5_hashes'])
Block = namedtuple('Block', ['md5_hash', 'compressed_bytes'])
OpStatus = namedtuple('OpStatus', ['transfers', 'current_index', 'last_index'])

class SiadError(Exception):
    def __init__(self, status, message):
        self.status = status
        self.message = message
    def __str__(self):
        return f'<Sia: [{self.status}] {self.message}>'
    def __repr__(self):
        return self.__str__()

class SiadSession():
    def __init__(self, domain, api_password):
        self.domain = domain
        self.api_password = api_password
    async def create(self):
        self.client = aiohttp.ClientSession(
                auth=aiohttp.BasicAuth('', password=self.api_password),
                timeout=aiohttp.ClientTimeout(total=None))
    async def close(self):
        await self.client.close()


def main():
    argp = ArgumentParser(
            description='Sync a large file to Sia with incremental updates.')
    argp_op = argp.add_mutually_exclusive_group(required=True)
    argp_op.add_argument(
            '-m', '--mirror', action='store_true',
            help=('sync a copy to Sia by dividing the file into '
                  f'{BLOCK_MB}MiB chunks'))
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
    def start(stdscr):
        nonlocal args
        asyncio.run(amain(stdscr, args))
    curses.wrapper(start)


async def amain(stdscr, args):
    session = SiadSession('http://localhost:9980', os.environ['SIAD_API'])
    await session.create()
    async def siapath():
        if not args.siapath:
            raise ValueError('no siapath specified')
        async def validate_sp(sp):
            response = await siad_post(session, b'', 'renter', 'validatesiapath', sp)
            return response.status == 204
        if not await validate_sp(args.siapath):
            raise ValueError(f'invalid siapath: {args.siapath}')
        return args.siapath.split('/')
    if args.mirror:
        await do_mirror(stdscr, session, args.file, await siapath())
    elif args.download:
        await do_download(stdscr, session, args.file, await siapath())
    elif args.resume:
        async with aiofile.AIOFile(args.file, 'rb') as state_afp:
            state_pickle = pickle.loads(await state_afp.read())
        if 'siaslice-mirror' in args.file:
            await do_mirror(
                    stdscr, session, state_pickle['source_file'],
                    state_pickle['siapath'],
                    start_block=state_pickle['current_index'])
        elif 'siaslice-download' in args.file:
            await do_download(
                    stdscr, session, state_pickle['target_file'],
                    state_pickle['siapath'], start_block=state_pickle['start_block'])
        else:
            raise ValueError(f'bad state file: {args.file}')
    await session.close()


async def do_mirror(stdscr, session, source_file, siapath, start_block=0):
    prior_map = await siapath_block_map(session, siapath)
    state_file = f"siaslice-mirror-{datetime.now().strftime('%Y%m%d-%H%M')}.dat"
    state_afp = aiofile.AIOFile(state_file, mode='wb')
    await state_afp.open()
    source_afp = aiofile.AIOFile(source_file, mode='rb')
    await source_afp.open()
    async for status in siapath_mirror(session, source_afp, siapath, prior_map,
                                       start_block=start_block):
        await state_afp.write(pickle.dumps({
                'source_file': source_file,
                'siapath': siapath,
                'block_size': prior_map.block_size,
                'current_index': status.current_index}))
        await state_afp.fsync()
        show_status(stdscr, status, title=f'{source_file} -> {format_sp(siapath)}')
    source_afp.close()
    state_afp.close()
    os.remove(state_file)


async def siapath_mirror(session, source_afp, siapath, prior_map, start_block=0):
    uploads = {}
    current_index = start_block
    update = asyncio.Event()

    async def read():
        nonlocal uploads, current_index, update
        async for index, block, change in \
                read_blocks(source_afp, prior_map, start_block):
            current_index = index
            if change:
                uploads[index] = 0.0
                update.set()
                up_siapath = \
                        siapath + [f'siaslice.{format_bs(prior_map.block_size)}.'
                                   f'{index}.{block.md5_hash}.lz']
                await siapath_delete_block(session, siapath, index)
                try:
                    await siad_post(session, BytesIO(block.compressed_bytes),
                                    'renter', 'uploadstream', *up_siapath)
                except:
                    await siapath_delete_block(session, siapath, index)
                    raise
                yield watch_upload(index, up_siapath)
            else:
                update.set()

    async def watch_upload(index, up_siapath):
        nonlocal uploads, update
        uploads[index] = 0.0
        update.set()
        up_status = {}
        while up_status.get('uploadprogress', 0) < 100:
            up_status = (await siad_json(await siad_get(
                    session, 'renter', 'file', *up_siapath))).get('file', {})
            uploads[index] = up_status.get('uploadprogress', 0)/100.0
            update.set()
            await asyncio.sleep(2)
        else:
            del uploads[index]
            update.set()

    main_done = False
    async def main():
        nonlocal read, main_done, update
        await run_all_tasks((task async for task in read()),
                            max_concurrent=MAX_CONCURRENT_UPLOADS)
        main_done = True
        update.set()

    main_task = asyncio.create_task(main())
    last_block = int(os.stat(source_afp.fileno()).st_size//prior_map.block_size)
    while not main_done:
        yield OpStatus(transfers=uploads, current_index=current_index,
                       last_index=last_block)
        await update.wait()
        update.clear()
    else:
        await main_task


async def siapath_delete_block(session, siapath, block_index):
    response = await siad_get(session, 'renter', 'dir', *siapath)
    if response.status == 500: # nonexistent directory
        pass
    elif response.status == 200:
        paths = (meta['siapath']
                 for meta in (await siad_json(response)).get('files', []))
        for path in paths:
            match = re.search(
                    rf'siaslice\.\d+MiB\.{block_index}\.[a-z\d]+\.lz$', path)
            if match:
                await siad_post(session, b'', 'renter', 'delete', *path.split('/'))
    else:
        raise ValueError(f"{format_sp(siapath)} is a file, not a directory")


async def read_blocks(source_afp, prior_block_map, start_block):
    block_size = prior_block_map.block_size
    index = start_block
    reader = aiofile.Reader(
            source_afp, chunk_size=block_size, offset=start_block*block_size)
    async for chunk in reader:
        md5_hash, lz_bytes = await asyncio.gather(aiomd5(chunk), aiolzc(chunk))
        block = Block(md5_hash=md5_hash.hexdigest(), compressed_bytes=lz_bytes)
        try:
            block_changed = block.md5_hash != prior_block_map.md5_hashes[index]
        except IndexError:
            block_changed = True
        yield (index, block, block_changed)
        index += 1


async def do_download(stdscr, session, target_file, siapath, start_block=0):
    state_file = f"siaslice-download-{datetime.now().strftime('%Y%m%d-%H%M')}.dat"
    state_afp = aiofile.AIOFile(state_file, mode='wb')
    await state_afp.open()
    target_afp = aiofile.AIOFile(target_file, mode='wb')
    await target_afp.open()
    async for status in siapath_download(session, target_afp, siapath,
                                         start_block=start_block):
        await state_afp.write(pickle.dumps({
                'target_file': target_file,
                'siapath': siapath,
                'current_index': status.current_index}))
        await state_afp.fsync()
        show_status(stdscr, status, title=f'{format_sp(siapath)} -> {source_file}')
    target_afp.close()
    state_afp.close()
    os.remove(state_file)


async def siapath_download(session, target_afp, siapath, start_block=0):
    downloads = {}
    current_index = start_block
    update = asyncio.Event()

    block_map = await siapath_block_map(session, siapath)
    md5_hashes = block_map.md5_hashes[start_block:]

    async def download(index, block_siapath):
        nonlocal block_map, downloads, current_index
        info = (await siad_json(await siad_get(
                session, 'renter', 'file', *block_siapath))).get('file', {})
        filesize = info.get('filesize', 0)
        written = 0
        async for chunk in siad_stream_lz(session, *block_siapath):
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
        nonlocal download, md5_hashes, main_done, update
        def block_siapath(index, md5_hash):
            return siapath + [f'siaslice.{format_bs(block_map.block_size)}'
                              f'.{index}.{md5_hash}.lz']
        await run_all_tasks(
                (download(index, block_siapath(index, md5_hash))
                 for index, md5_hash in enumerate(md5_hashes) if md5_hash),
                max_concurrent=MAX_CONCURRENT_DOWNLOADS)
        main_done = True
        update.set()

    main_task = asyncio.create_task(main())
    last_block = len(block_map.md5_hashes) - 1
    while not main_done:
        yield OpStatus(transfers=downloads, current_index=current_index,
                       last_index=last_block)
        await update.wait()
        update.clear()
    else:
        await main_task


async def siapath_block_map(
        session, siapath, fallback_block_size=BLOCK_MB*1e3*1e3):
    response = await siad_get(session, 'renter', 'dir', *siapath)
    if response.status == 500: # nonexistent directory
        return BlockMap(block_size=fallback_block_size, md5_hashes=[])
    elif response.status == 200:
        filenames = (meta['siapath'].split('/')[-1:][0]
                     for meta in (await siad_json(response)).get('files', []))
        block_size = fallback_block_size
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
        raise ValueError(f"{format_sp(siapath)} is a file, not a directory")


async def siad_stream_lz(session, *siapath):
    response = await siad_get(session, 'renter', 'stream', *siapath)
    alzd = aioify(obj=LZMADecompressor().decompress)
    while True:
        chunk = await response.content.read(1*1000*1000)
        if chunk:
            yield alzd(chunk)
        else:
            break


def format_bs(block_size):
    n = int(block_size/1e3/1e3)
    return f'{n}MiB'

def format_sp(siapath): return '/'.join(siapath)


def show_status(stdscr, status, title=''):
    stdscr.refresh()
    lines, cols = stdscr.getmaxyx()
    curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)

    if status.last_index > 0:
        blocks = f'block {status.current_index} / {status.last_index}'
    else:
        blocks = f'block {status.current_index}'
    stdscr.addstr(0, 0, ' '*cols, curses.color_pair(1))
    stdscr.addstr(0, 0, title[:cols], curses.color_pair(1))
    stdscr.addstr(0, max(cols - len(blocks) - 1, 0), ' ' + blocks,
                  curses.color_pair(1))

    visible_transfers = min(len(status.transfers), lines - 2)
    transfers = sorted(status.transfers.items())[:visible_transfers]
    def progress_bar(y, block, pct):
        bar_size = max(cols - 11 - 4, 10)
        stdscr.addstr(y, 0, f'{block: 10} ')
        stdscr.addstr(y, 11, f"[{' '*(bar_size - 2)}]")
        stdscr.addstr(y, 11 + 1, f"{'='*round(pct*(bar_size - 3))}>")
        stdscr.addstr(y, cols - 4, f'{round(pct*100.0): 3}%')
    for l in range(lines - 2):
        try:
            progress_bar(l + 1, *transfers[l])
        except IndexError:
            stdscr.addstr(l + 1, 0, ' '*cols)

    stdscr.refresh()


async def siad_get(session, *path, **qs):
    url = f"{session.domain}/{'/'.join(path)}"
    headers = { 'User-Agent': USER_AGENT }
    return await session.client.get(url, params=qs, headers=headers)


async def siad_post(session, post_data, *path, **qs):
    url = f"{session.domain}/{'/'.join(path)}"
    headers = { 'User-Agent': USER_AGENT }
    return await session.client.post(url, data=post_data, params=qs, headers=headers)


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

