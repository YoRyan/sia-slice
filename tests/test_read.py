from hashlib import md5
from lzma import compress

import aiofile
import asynctest

import siaslice as ss


class BaseTestCases:

    class FourStripeTarget(asynctest.TestCase):

        async def setUp(self):
            await self.afp.open()
            reader = aiofile.Reader(self.afp, chunk_size=self.block_size)
            self.md5_hashes = []
            self.compressed = []
            async for chunk in reader:
                self.md5_hashes.append(md5(chunk).hexdigest())
                self.compressed.append(compress(chunk))

        async def test_initial_upload(self):
            prior_map = ss.BlockMap(
                    block_size=self.block_size, md5_hashes=[])
            async for index, block, change in ss.read_blocks(
                    self.afp, prior_map, 0):
                self.assertEqual(block.md5_hash, self.md5_hashes[index])
                self.assertEqual(block.lzma_reader.read(), self.compressed[index])
                self.assertTrue(change)

        async def test_all_change(self):
            prior_map = ss.BlockMap(
                    block_size=self.block_size, md5_hashes=['x']*4)
            async for index, block, change in ss.read_blocks(
                    self.afp, prior_map, 0):
                self.assertEqual(block.md5_hash, self.md5_hashes[index])
                self.assertEqual(block.lzma_reader.read(), self.compressed[index])
                self.assertTrue(change)

        async def test_partial_change(self):
            prior_map = ss.BlockMap(
                    block_size=self.block_size,
                    md5_hashes=[self.md5_hashes[0], 'x',
                                'x', self.md5_hashes[3]])
            async for index, block, change in ss.read_blocks(
                    self.afp, prior_map, 0):
                self.assertEqual(block.md5_hash, self.md5_hashes[index])
                self.assertEqual(block.lzma_reader.read(), self.compressed[index])
                if index == 1 or index == 2:
                    self.assertTrue(change)
                else:
                    self.assertFalse(change)

        async def test_no_change(self):
            prior_map = ss.BlockMap(
                    block_size=self.block_size, md5_hashes=self.md5_hashes)
            async for index, block, change in ss.read_blocks(
                    self.afp, prior_map, 0):
                self.assertEqual(block.md5_hash, self.md5_hashes[index])
                self.assertEqual(block.lzma_reader.read(), self.compressed[index])
                self.assertFalse(change)

        async def test_offset_start(self):
            prior_map = ss.BlockMap(
                    block_size=self.block_size,
                    md5_hashes=(['x']*3 + [self.md5_hashes[3]]))
            async for index, block, change in ss.read_blocks(
                    self.afp, prior_map, 2):
                self.assertEqual(block.md5_hash, self.md5_hashes[index])
                self.assertEqual(block.lzma_reader.read(), self.compressed[index])
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
        prior_map = ss.BlockMap(block_size=1024, md5_hashes=[])
        async for index, block, change in ss.read_blocks(
                self.afp, prior_map, 0):
            self.fail('read a block from an empty file')

    async def tearDown(self):
        await self.afp.close()


if __name__ == '__main__':
    asynctest.main()

