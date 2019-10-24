from asyncio import sleep
from hashlib import md5
from lzma import compress

import asynctest

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


class TestSyncGenerators(asynctest.TestCase):

    def test_fp_generator(self):
        with open('40MiBempty.img', 'rb') as fp:
            reference = fp.read()
            fp.seek(0)
            read = b''.join(chunk for chunk in ss.region_read(fp, 40*1000*1000))
        self.assertEqual(read, reference)

    def test_generator_stream(self):
        def gen():
            for i in range(10):
                yield f'{i}'.encode()*1000
        reference = b''.join(bytez for bytez in gen())
        stream = ss.GeneratorStream(gen())
        self.assertEqual(stream.read(), reference)

    def test_md5_hasher(self):
        with open('40MiBempty.img', 'rb') as fp:
            reference = md5(fp.read()).hexdigest()
            fp.seek(0)
            compare = ss.md5_hasher(ss.region_read(fp, 40*1000*1000))
        self.assertEqual(compare, reference)

    def test_lzma_compress(self):
        with open('40MiBempty.img', 'rb') as fp:
            reference = compress(fp.read())
            fp.seek(0)
            compare = b''.join(chunk for chunk in
                               ss.lzma_compress(ss.region_read(fp, 40*1000*1000)))
        self.assertEqual(compare, reference)


if __name__ == '__main__':
    asynctest.main()

