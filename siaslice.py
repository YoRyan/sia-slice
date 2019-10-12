#!/usr/bin/env python3

import asyncio
import lzma
import os
import re
from argparse import ArgumentParser
from collections import namedtuple
from datetime import datetime
from hashlib import md5

from aioify import aioify
import aiofile
import aiohttp


aiomd5 = aioify(obj=md5)
aiolzc = aioify(obj=lzma.compress)
aiolzd = aioify(obj=lzma.decompress)

BlockMap = namedtuple('BlockMap', ['block_size', 'md5_hashes'])
Block = namedtuple('Block', ['md5_hash', 'compressed_bytes'])
SiadEndpoint = namedtuple('SiadEndpoint', ['domain', 'api_password'])

DEFAULT_BLOCK_MB = 40
USER_AGENT = 'Sia-Agent'


def main(*arg, **kwarg):
    asyncio.run(amain(*arg, **kwarg))

async def amain():
    argp = ArgumentParser(
            description='Sync a large file to Sia with incremental updates.')
    argp_xfer = argp.add_mutually_exclusive_group()
    argp_xfer.add_argument(
            '-r', '--resume', dest='logfile',
            help='resume a stalled sync operation using the log file')
    argp_xfer.add_argument(
            '-b', '--block-size', dest='mb', type=int, default=DEFAULT_BLOCK_MB,
            help='divide the file into chunks of MB megabytes (once set, cannot '
                 + f'be changed; defaults to {DEFAULT_BLOCK_MB}MB)')
    argp.add_argument(
            'file', help='the file to upload to Sia')
    argp.add_argument(
            'siapath',
            help='the file will be stored as a directory at this Sia location')
    args = argp.parse_args()

    if args.logfile:
        # (Currently we don't do anything with the log file except locate the
        #  last successfully transferred block.)
        with aiofile.AIOFile(args.logfile, 'rt') as log_afp:
            saved_block_map = await read_map_file(aiofile.Reader(log_afp))
            start_block = len(saved_block_map.md5_hashes)
    else:
        start_block = 0
    siapath = args.siapath.split('/')
    endpoint = SiadEndpoint(domain='http://localhost:9980',
                            api_password=os.environ['SIAD_API'])
    async with aiofile.AIOFile(args.file, mode='rb') as source_afp:
        await do_sia_sync(endpoint, source_afp, siapath, last_map)


async def read_map_file(afp):
    block_line = await afp.readline()
    block_match = re.search(r'^([0-9]+)M$', block_line)
    if block_match is None:
        raise ValueError(f'invalid block size: {block_line}')
    block_size = int(block_match.group(1))*1024*1024
    md5_hashes = [line async for line in afp]
    return BlockMap(block_size=block_size, md5_hashes=md5_hashes)


async def do_sia_sync(
        endpoint, source_afp, siapath, block_size, start_block=0):
    # Retrieve the initial block map from Sia, if it exists.
    map_siapath = siapath + ['siaslice.map']
    map_response = await siad_get(endpoint, 'renter', 'stream', *map_siapath)
    if True:
        prior_map = read_map_file(map_response.content)
        assert prior_map.block_size == block_size
    else:
        prior_map = BlockMap(block_size=block_size, md5_hashes=[])

    # Start reading blocks; write to the log file.
    current_map = prior_map.copy()
    timestamp = datetime.now().strftime('%Y%m%d-%H%M')
    log_file = f'siaslice-{timestamp}.log'
    async for index, block, change in read_blocks(
            source_afp, prior_map, start_block):
        try:
            current_map.md5_hashes[index] = block.md5_hash
        except IndexError:
            assert index == len(current_map.md5_hashes)
            current_map.md5_hashes.append(block.md5_hash)
        current_map_bytes = save_map_file(current_map)

        if change:
            # Upload new block to Sia.
            block_siapath = siapath + [f'siaslice.lz.{index}']
            await siad_post(
                    endpoint, block.compressed_bytes,
                    'renter', 'uploadstream', *block_siapath, force='')
            # Upload new map file to Sia (do this every time to stay consistent).
            await siad_post(
                    endpoint, current_map_bytes,
                    'renter', 'uploadstream', *map_siapath, force='')

        async with aiofile.AIOFile(log_file, 'wt'):
            writer = aiofile.Writer(source_afp)
            await writer(current_map_bytes)

    os.remove(log_file)


def save_map_file(block_map):
    binary = f'{block_map.block_size}MB\n'.encode()
    for md5_hash in block_map.md5_hashes:
        binary += f'{md5_hash}\n'.encode()
    return binary


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


async def read_blocks(source_afp, prior_block_map, start_block):
    # Read, hash, and compress blocks in the background.
    block_size = prior_block_map.block_size
    read_queue = asyncio.Queue(maxsize=2)
    async def read_worker():
        reader = aiofile.Reader(
                source_afp, chunk_size=block_size, offset=start_block*block_size)
        async for chunk in reader:
            md5_hash, lz_bytes = await asyncio.gather(aiomd5(chunk), aiolzc(chunk))
            await read_queue.put(Block(
                    md5_hash=md5_hash.hexdigest(), compressed_bytes=lz_bytes))
        await read_queue.put(None) # end-of-read sentinel
    read_task = asyncio.create_task(read_worker())

    # Dequeue blocks and detect changes.
    index = start_block
    block = await read_queue.get()
    while block is not None:
        try:
            prior_hash = prior_block_map.md5_hashes[index]
        except IndexError:
            prior_hash = ''

        block_changed = prior_hash != block.md5_hash
        yield (index, block, block_changed)

        index += 1
        block = await read_queue.get()
    read_task.cancel()


if __name__ == '__main__':
    main()
