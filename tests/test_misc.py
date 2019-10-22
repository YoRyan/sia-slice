from asyncio import sleep
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

    async def test_limit_concurrency(self,):
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


class TestLZMA(asynctest.TestCase):

    def test_reader(self):
        data = b'The quick brown fox jumps over the brown lazy dog.'
        data_lz = compress(data)

        readback = b''
        reader = ss.LZMACompressReader(data)
        while True:
            chunk = reader.read(4)
            if chunk:
                readback += chunk
            else:
                break
        self.assertEqual(data_lz, readback)


if __name__ == '__main__':
    asynctest.main()

