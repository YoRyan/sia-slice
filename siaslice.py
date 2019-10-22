#!/usr/bin/env python3

import asyncio
import curses
import os
import pickle
import re
from argparse import ArgumentParser
from collections import namedtuple
from hashlib import md5
from io import IOBase
from lzma import LZMACompressor, LZMADecompressor
from types import AsyncGeneratorType, GeneratorType

import aiofile
import aiohttp
import pendulum
from aioify import aioify


aiomd5 = aioify(obj=md5)

BLOCK_MB = 100
TRANSFER_STALLED_MIN = 3*60

OpStatus = namedtuple('OpStatus', ['transfers', 'current_index', 'last_index'])


class SiadSession():
    USER_AGENT = 'Sia-Agent'
    MAX_CONCURRENT_UPLOADS = 1
    MAX_CONCURRENT_DOWNLOADS = 10
    CHUNK_SZ = 500*1000

    def __init__(self, domain, api_password):
        self._client = None
        self._domain = domain
        self._api_password = api_password
        self._upload_sem = asyncio.BoundedSemaphore(
                value=SiadSession.MAX_CONCURRENT_UPLOADS)
        self._download_sem = asyncio.BoundedSemaphore(
                value=SiadSession.MAX_CONCURRENT_DOWNLOADS)

    async def create(self):
        self._client = aiohttp.ClientSession(
                auth=aiohttp.BasicAuth('', password=self._api_password),
                timeout=aiohttp.ClientTimeout(total=None))

    async def close(self):
        await self._client.close()

    async def get(self, *path, **qs):
        headers = {'User-Agent': SiadSession.USER_AGENT}
        return await self._client.get(f"{self._domain}/{'/'.join(path)}",
                                      params=qs, headers=headers)

    async def post(self, data, *path, **qs):
        headers = {'User-Agent': SiadSession.USER_AGENT}
        return await self._client.post(f"{self._domain}/{'/'.join(path)}",
                                       data=data, params=qs, headers=headers)

    async def upload(self, siapath, data):
        part_siapath = siapath[:-1] + (f'{siapath[-1]}.part',)
        async with self._upload_sem:
            await self.post(data, 'renter', 'uploadstream', *part_siapath)
            await self.post(b'', 'renter', 'rename',
                            *part_siapath, newsiapath=format_sp(siapath))

    async def download(self, siapath):
        async with self._download_sem:
            response = await self.get('renter', 'stream', *siapath)
            while True:
                chunk = await response.content.read(SiadSession.CHUNK_SZ)
                if chunk:
                    yield chunk
                else:
                    break


class SiapathStorage():
    _BlockFile = namedtuple('_BlockFile', ['siapath', 'md5_hash', 'size', 'partial',
                                           'complete', 'stalled', 'upload_progress'])

    def __init__(self, session, *siapath, default_block_size=BLOCK_MB*1000*1000):
        self._session = session
        self._siapath = siapath
        self.block_size = default_block_size
        self.block_files = {}

    async def update(self):
        response = await self._session.get('renter', 'dir', *self._siapath)
        if response.status == 500: # nonexistent directory
            self.block_files = {}
        elif response.status == 200:
            siafiles = (await response.json()).get('files', [])
            block_size = None
            block_files = {}
            for siafile in siafiles:
                file_match = re.search(
                        r'/siaslice\.(\d+)MiB\.(\d+)\.([a-z\d]+)\.lz(\.part)?$',
                        siafile['siapath'])
                if not file_match:
                    continue

                file_index = int(file_match.group(2))
                if file_index in block_files:
                    raise ValueError(f'duplicate files found for block {file_index}')

                file_block_size = int(file_match.group(1))*1000*1000
                if not block_size:
                    block_size = file_block_size
                elif block_size != file_block_size:
                    raise ValueError(
                            f'inconsistent block sizes at {siafile.siapath} - '
                            f'found {file_block_size}B, expected {block_size}B')

                file_md5_hash = file_match.group(3)
                file_partial = file_match.group(4) is not None

                file_age = pendulum.now() - pendulum.parse(siafile['createtime'])
                block_files[file_index] = SiapathStorage._BlockFile(
                        siapath=tuple(siafile['siapath'].split('/')),
                        md5_hash=file_md5_hash,
                        size=siafile['filesize'],
                        partial=file_partial,
                        complete=siafile['available'],
                        stalled=(not siafile['available']
                                 and file_age.minutes >= TRANSFER_STALLED_MIN),
                        upload_progress=siafile['uploadprogress']/100.0)
            self.block_files = block_files
        else:
            raise NotADirectoryError

    async def delete(self, index):
        if index in self.block_files:
            siapath = self.block_files[index].siapath
            await self._session.post(b'', 'renter', 'delete', *siapath)
        await self.update()

    async def upload(self, index, data):
        md5_hash = (await aiomd5(data)).hexdigest()
        filename = f'siaslice.{format_bs(self.block_size)}.{index}.{md5_hash}.lz'
        await self._session.upload(self._siapath + (filename,),
                                   LZMACompressReader(data))
        await self.update()

    async def download(self, index):
        await self.update()
        if index not in self.block_files:
            raise FileNotFoundError
        block_file = self.block_files[index]
        if block_file.partial or not block_file.complete:
            raise FileNotFoundError

        alzd = aioify(obj=LZMADecompressor().decompress)
        async for chunk in self._session.download(block_file.siapath):
            yield alzd(chunk)


class LZMACompressReader(IOBase):
    CHUNK_SZ = 500*1000

    def __init__(self, data):
        self._data = data
        self._lzdata = b''
        self._dataptr = self._lzptr = 0
        self._compressor = LZMACompressor()

    def readable(self):
        return True
    def read(self, size=-1):
        if size < 0:
            while True:
                try:
                    self._compress()
                except EOFError:
                    lzdata = self._lzdata[self._lzptr:]
                    self._lzptr = len(self._lzdata)
                    return lzdata
        else:
            while len(self._lzdata) < self._lzptr + size:
                try:
                    self._compress()
                except EOFError:
                    break
            lzdata = self._lzdata[self._lzptr:self._lzptr + size]
            self._lzptr = min(len(self._lzdata), self._lzptr + size)
            return lzdata

    def _compress(self):
        if self._dataptr >= len(self._data):
            try:
                self._lzdata += self._compressor.flush()
            except ValueError:
                raise EOFError
        else:
            self._lzdata += self._compressor.compress(
                    self._data[self._dataptr:
                               self._dataptr + LZMACompressReader.CHUNK_SZ])
            self._dataptr += LZMACompressReader.CHUNK_SZ


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
            response = await session.post(b'', 'renter', 'validatesiapath', sp)
            return response.status == 204
        if not await validate_sp(args.siapath):
            raise ValueError(f'invalid siapath: {args.siapath}')
        return tuple(args.siapath.split('/'))
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
    state_file = f"siaslice-mirror-{pendulum.now().strftime('%Y%m%d-%H%M')}.dat"
    state_afp = aiofile.AIOFile(state_file, mode='wb')
    await state_afp.open()

    source_afp = aiofile.AIOFile(source_file, mode='rb')
    await source_afp.open()
    storage = SiapathStorage(session, *siapath)
    await storage.update()

    async for status in siapath_mirror(storage, source_afp, start_block=start_block):
        await state_afp.write(pickle.dumps({
                'source_file': source_file,
                'siapath': siapath,
                'current_index': status.current_index}))
        await state_afp.fsync()
        show_status(stdscr, status, title=f'{source_file} -> {format_sp(siapath)}')
    source_afp.close()
    state_afp.close()
    os.remove(state_file)


async def siapath_mirror(storage, source_afp, start_block=0):
    current_index = 0
    transfers = {}
    status = asyncio.Condition()

    async def linear_read():
        nonlocal status, current_index
        index = start_block
        reader = aiofile.Reader(source_afp, chunk_size=storage.block_size,
                                offset=start_block*storage.block_size)
        async for chunk in reader:
            async with status:
                current_index = index
                status.notify()
            md5_hash = (await aiomd5(chunk)).hexdigest()
            if (index not in storage.block_files
                    or storage.block_files[index].md5_hash != md5_hash
                    or storage.block_files[index].partial):
                await storage.delete(index)
                await storage.upload(index, chunk)
            index += 1

    async def watch_storage():
        nonlocal status, transfers, current_index, reupload, read_task
        REUPLOAD_TIME = 5*60
        reupload_task = None
        uploads_done = False
        while True:
            await asyncio.sleep(5)
            await storage.update()
            async with status:
                transfers = {index: bf.upload_progress for index, bf
                             in storage.block_files.items() if not bf.complete}
                uploads_done = transfers == {}
                status.notify()

            if reupload_task is None or reupload_task.done():
                if reupload_task is not None:
                    await reupload_task
                stalled_upload = next(
                        (index for index, bf in storage.block_files.items()
                         if bf.stalled and index < current_index), None)
                if stalled_upload is not None:
                    reupload_task = asyncio.create_task(reupload(stalled_upload))
                elif uploads_done and read_task.done():
                    async with status:
                        status.notify()
                    break

    async def reupload(index):
        chunk = await source_afp.read(storage.block_size,
                                      offset=index*storage.block_size)
        await storage.delete(index)
        await storage.upload(index, chunk)

    read_task = asyncio.create_task(linear_read())
    watch_task = asyncio.create_task(watch_storage())
    last_block = int(os.stat(source_afp.fileno()).st_size//storage.block_size)
    async with status:
        while not watch_task.done():
            await status.wait()
            yield OpStatus(transfers=transfers, last_index=last_block,
                           current_index=current_index)
    await read_task
    await watch_task

    # Trim extraneous blocks in the event of a shrunken source.
    # Can be *dangerous* if the user made a mistake, so wait a minute first.
    trim_indices = (index for index in source.block_files.keys()
                    if index > current_index)
    to_trim = next(trim_indices, None)
    if to_trim is not None:
        await asyncio.sleep(60)
        await storage.delete(to_trim)
        for to_trim in trim_indices:
            await storage.delete(to_trim)


async def do_download(stdscr, session, target_file, siapath, start_block=0):
    state_file = f"siaslice-download-{pendulum.now().strftime('%Y%m%d-%H%M')}.dat"
    state_afp = aiofile.AIOFile(state_file, mode='wb')
    await state_afp.open()

    target_afp = aiofile.AIOFile(target_file, mode='r+b')
    await target_afp.open()
    storage = SiapathStorage(session, *siapath)

    async for status in siapath_mirror(storage, target_afp, start_block=start_block):
        await state_afp.write(pickle.dumps({
                'target_file': target_file,
                'siapath': siapath,
                'current_index': status.current_index}))
        await state_afp.fsync()
        show_status(stdscr, status, title=f'{format_sp(siapath)} -> {source_file}')
    target_afp.close()
    state_afp.close()
    os.remove(state_file)


async def siapath_download(storage, target_afp, start_block=0):
    current_index = 0
    transfers = {}
    finished = False
    status = asyncio.Condition()

    async def parallel_download():
        nonlocal status, transfers, download, finished
        for index, block_file in storage.block_files.items():
            async with status:
                transfers[index] = 0.0
                status.notify()
            yield download(index, block_file)
        async with status:
            finished = True
            status.notify()

    async def download(index, block_file):
        nonlocal status, transfers, current_index
        written = 0
        async for chunk in storage.download(index):
            await target_afp.write(chunk, offset=index*storage.block_size + written)
            written += len(chunk)
            async with status:
                transfers[index] = (written/block_file.size
                                    if block_file.size > 0 else 0.0)
                status.notify()
        async with status:
            del transfers[index]
            current_index = min(transfers.keys()) if transfers != {} else index
            status.notify()

    download_task = asyncio.create_task(await_all(limit_concurrency(
            (task async for task in parallel_download()),
            SiadSession.MAX_CONCURRENT_DOWNLOADS)))
    async with status:
        while not finished:
            await status.wait()
            yield OpStatus(transfers=transfers, current_index=current_index,
                           last_index=len(storage.block_files) - 1)
    await download_task


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


def limit_concurrency(generator, limit):
    sem = asyncio.BoundedSemaphore(value=limit)
    async def wrap_sync(the_gen):
        nonlocal sem
        for cor in the_gen:
            await sem.acquire()
            yield finish_task(cor)
    async def wrap_async(the_gen):
        nonlocal sem
        async for cor in the_gen:
            await sem.acquire()
            yield finish_task(cor)
    async def finish_task(the_cor):
        nonlocal sem
        await the_cor
        sem.release()
    if isinstance(generator, GeneratorType):
        return wrap_sync(generator)
    elif isinstance(generator, AsyncGeneratorType):
        return wrap_async(generator)


async def await_all(generator):
    if isinstance(generator, GeneratorType):
        await asyncio.gather(*generator)
    elif isinstance(generator, AsyncGeneratorType):
        running = 0
        cv = asyncio.Condition()

        async def finish_task(the_cor):
            nonlocal running, cv
            await the_cor
            running -= 1
            async with cv:
                cv.notify()
        async for cor in generator:
            running += 1
            asyncio.create_task(finish_task(cor))
        async with cv:
            await cv.wait_for(lambda: running == 0)
    else:
        raise ValueError(f'not a generator: {generator}')


if __name__ == '__main__':
    main()

