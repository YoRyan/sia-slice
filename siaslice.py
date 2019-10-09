#!/usr/bin/env python3

import asyncio
import lzma
import re
from argparse import ArgumentParser
from collections import namedtuple
from hashlib import md5

from aioify import aioify
import aiofile


aiomd5 = aioify(obj=md5)
aiolzc = aioify(obj=lzma.compress)
aiolzd = aioify(obj=lzma.decompress)

BlockMap = namedtuple('BlockMap', ['block_size', 'md5_hashes'])
Block = namedtuple('Block', ['md5_hash', 'compressed_bytes'])

DEFAULT_BLOCK_MB = 40


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
        with open(args.logfile, 'rt') as log_fp:
            last_map = read_map_file(log_fp)
    else:
        last_map = BlockMap(block_size=args.mb*1024*1024, md5_hashes=[])
    siapath = args.siapath
    async with aiofile.AIOFile(args.file, mode='rb') as source_afp:
        await do_sync(source_afp, siapath, last_map)


def read_map_file(fp):
    block_line = fp.readline()
    block_match = re.search(r'^([0-9]+)M$', fp.readline())
    if block_match is None:
        raise ValueError(f'invalid block size: {block_line}')
    block_size = int(block_match.group(1))*1024*1024
    md5_hashes = [line for line in fp]
    return BlockMap(block_size=block_size, md5_hashes=md5_hashes)


async def do_sync(source_afp, siapath, last_map):
    # (Currently we don't do anything with last_map except locate the last
    #  successfully transferred block.)
    start_block = len(last_map.md5_hashes)


async def crawl_and_mirror(source_afp, prior_map, update_callback, start_block=0):
    block_size = prior_map.block_size

    # Read, hash, and compress blocks in the background
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

    # Dequeue blocks; if the prior hash doesn't match, update the block
    index = start_block
    block = await read_queue.get()
    while block is not None:
        try:
            prior_hash = prior_map.md5_hashes[index]
        except IndexError:
            prior_hash = None
        if prior_hash != block.md5_hash:
            await update_callback(index, block)
        index += 1
        block = await read_queue.get()

    read_task.cancel()


if __name__ == '__main__':
    main()
