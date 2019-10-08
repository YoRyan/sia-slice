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
            prior_map = siaslice.BlockMap(block_size=self.block_size, md5_hashes=[])
            mock_callback = asynctest.CoroutineMock()
            await siaslice.crawl_and_mirror(self.afp, prior_map, mock_callback)
            calls = [asynctest.call(i, block)
                     for i, block in enumerate(self.blocks)]
            mock_callback.assert_has_calls(calls)

        async def test_all_change(self):
            prior_map = siaslice.BlockMap(
                    block_size=self.block_size, md5_hashes=['x']*4)
            mock_callback = asynctest.CoroutineMock()
            await siaslice.crawl_and_mirror(self.afp, prior_map, mock_callback)
            calls = [asynctest.call(i, block)
                     for i, block in enumerate(self.blocks)]
            mock_callback.assert_has_calls(calls)

        async def test_partial_change(self):
            prior_map = siaslice.BlockMap(
                    block_size=self.block_size,
                    md5_hashes=[self.blocks[0].md5_hash, 'x',
                                'x', self.blocks[3].md5_hash])
            mock_callback = asynctest.CoroutineMock()
            await siaslice.crawl_and_mirror(self.afp, prior_map, mock_callback)
            calls = [asynctest.call(1, self.blocks[1]),
                     asynctest.call(2, self.blocks[2])]
            mock_callback.assert_has_calls(calls)

        async def test_no_change(self):
            prior_map = siaslice.BlockMap(
                    block_size=self.block_size,
                    md5_hashes=[block.md5_hash for block in self.blocks])
            mock_callback = asynctest.CoroutineMock()
            await siaslice.crawl_and_mirror(self.afp, prior_map, mock_callback)
            mock_callback.assert_not_awaited()

        async def tearDown(self):
            await self.afp.close()


class SmallBlocksTarget(BaseTestCases.FourStripeTarget):

    async def setUp(self):
        self.afp = aiofile.AIOFile('20Kstripe.img', mode='rb')
        self.block_size = 20*1024
        await super().setUp()


class BigBlocksTarget(BaseTestCases.FourStripeTarget):

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
        mock_callback = asynctest.CoroutineMock()
        await siaslice.crawl_and_mirror(
                self.afp, prior_map, mock_callback, start_block=0)
        mock_callback.assert_not_awaited()

    async def tearDown(self):
        await self.afp.close()
