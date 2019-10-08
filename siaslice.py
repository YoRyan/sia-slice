#!/usr/bin/env python3

import asyncio
import lzma
from collections import namedtuple
from hashlib import md5

from aioify import aioify
import aiofile


BlockMap = namedtuple('BlockMap', ['block_size', 'md5_hashes'])
Block = namedtuple('Block', ['md5_hash', 'compressed_bytes'])

aiomd5 = aioify(obj=md5)
aiolzc = aioify(obj=lzma.compress)
aiolzd = aioify(obj=lzma.decompress)


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

def main():
    print('Hello World!')

if __name__ == '__main__':
    main()
