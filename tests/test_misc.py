from asyncio import sleep
from hashlib import md5
from lzma import compress

import asynctest
from aiofile import AIOFile

import siaslice as ss


class TestTaskGenerator(asynctest.TestCase):

    async def test_await_all(self):
        mock = asynctest.CoroutineMock()
        async def cor():
            await mock(1)
        await ss.await_all(cor() for i in range(10))
        mock.assert_has_awaits([asynctest.call(1)]*10)

    async def test_limit_concurrency(self):
        mock = asynctest.CoroutineMock()
        v = 0
        async def cor():
            nonlocal v
            new_v = v + 1
            await sleep(1)
            v = new_v
            await mock(v)
        await ss.await_all(ss.limit_concurrency((cor() for i in range(10)), 1))
        mock.assert_has_awaits([asynctest.call(i) for i in range(1, 11)])


class TestGenerators(asynctest.TestCase):

    async def test_afp_generator(self):
        async with AIOFile('40MiBempty.img', mode='rb') as afp:
            reference = await afp.read()
            read = b''.join([chunk async for chunk
                             in ss.region_read(afp, 0, 40*1000*1000)])
        self.assertEqual(read, reference)

    async def test_is_zeroes(self):
        async with AIOFile('40MiBempty.img', mode='rb') as afp:
            self.assertTrue(await ss.is_zeroes(ss.region_read(afp, 0, 40*1000*1000)))

    async def test_is_not_zeroes(self):
        async def agen(gen):
            for x in gen:
                yield x
        chunks = [b'\0\0\0\0', b'\0\0\0\0', b'\0\0\0X', b'\0\0\0\0']
        self.assertFalse(await ss.is_zeroes(agen(iter(chunks))))

    async def test_md5_hasher(self):
        async with AIOFile('40MiBempty.img', mode='rb') as afp:
            reference = md5(await afp.read()).hexdigest()
            compare = await ss.md5_hasher(ss.region_read(afp, 0, 40*1000*1000))
        self.assertEqual(compare, reference)

    async def test_lzma_compress(self):
        async with AIOFile('40MiBempty.img', mode='rb') as afp:
            reference = compress(await afp.read())
            agen = ss.region_read(afp, 0, 40*1000*1000)
            compare = b''.join([chunk async for chunk in ss.lzma_compress(agen)])
        self.assertEqual(compare, reference)


if __name__ == '__main__':
    asynctest.main()

