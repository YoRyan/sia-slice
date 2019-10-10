import lzma
from hashlib import md5

import aiofile
import asynctest

import siaslice


class BaseTestCases:

    class FourStripeTarget(asynctest.TestCase):

        async def setUp(self):
            await self.afp.open()
            reader = aiofile.Reader(self.afp, chunk_size=self.block_size)
            self.blocks = [siaslice.Block(md5_hash=md5(chunk).hexdigest(),
                                          compressed_bytes=lzma.compress(chunk))
                           async for chunk in reader]

        async def test_initial_upload(self):
            prior_map = siaslice.BlockMap(
                    block_size=self.block_size, md5_hashes=[])
            async for index, block, change in siaslice.read_blocks(
                    self.afp, prior_map, 0):
                self.assertEqual(block, self.blocks[index])
                self.assertTrue(change)

        async def test_all_change(self):
            prior_map = siaslice.BlockMap(
                    block_size=self.block_size, md5_hashes=['x']*4)
            async for index, block, change in siaslice.read_blocks(
                    self.afp, prior_map, 0):
                self.assertEqual(block, self.blocks[index])
                self.assertTrue(change)

        async def test_partial_change(self):
            prior_map = siaslice.BlockMap(
                    block_size=self.block_size,
                    md5_hashes=[self.blocks[0].md5_hash, 'x',
                                'x', self.blocks[3].md5_hash])
            async for index, block, change in siaslice.read_blocks(
                    self.afp, prior_map, 0):
                self.assertEqual(block, self.blocks[index])
                if index == 1 or index == 2:
                    self.assertTrue(change)
                else:
                    self.assertFalse(change)

        async def test_no_change(self):
            prior_map = siaslice.BlockMap(
                    block_size=self.block_size,
                    md5_hashes=[block.md5_hash for block in self.blocks])
            async for index, block, change in siaslice.read_blocks(
                    self.afp, prior_map, 0):
                self.assertEqual(block, self.blocks[index])
                self.assertFalse(change)

        async def test_offset_start(self):
            prior_map = siaslice.BlockMap(
                    block_size=self.block_size,
                    md5_hashes=(['x']*3 + [self.blocks[3].md5_hash]))
            async for index, block, change in siaslice.read_blocks(
                    self.afp, prior_map, 2):
                self.assertEqual(block, self.blocks[index])
                if index == 2:
                    self.assertTrue(change)
                else:
                    self.assertFalse(change)

        async def tearDown(self):
            await self.afp.close()


class SmallBlockStripes(BaseTestCases.FourStripeTarget):

    async def setUp(self):
        self.afp = aiofile.AIOFile('20Kstripe.img', mode='rb')
        self.block_size = 20*1024
        await super().setUp()


class BigBlockStripes(BaseTestCases.FourStripeTarget):

    async def setUp(self):
        self.afp = aiofile.AIOFile('40Mstripe.img', mode='rb')
        self.block_size = 40*1024*1024
        await super().setUp()


class ZeroLengthTarget(asynctest.TestCase):

    async def setUp(self):
        self.afp = aiofile.AIOFile('empty.img', mode='rb')
        await self.afp.open()

    async def test_zero_length(self):
        prior_map = siaslice.BlockMap(block_size=1024, md5_hashes=[])
        async for index, block, change in siaslice.read_blocks(
                self.afp, prior_map, 0):
            self.fail('read a block from an empty file')

    async def tearDown(self):
        await self.afp.close()


if __name__ == '__main__':
    asynctest.main()
