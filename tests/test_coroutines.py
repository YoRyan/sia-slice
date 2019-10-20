import asynctest
from asyncio import sleep

import siaslice as ss


class TestTaskGenerator(asynctest.TestCase):

    async def test_await_all(self):
        mock = asynctest.CoroutineMock()
        async def cor():
            await mock(1)
        await ss.run_all_tasks(cor() for i in range(10))
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
        await ss.run_all_tasks(ss.limit_concurrency((cor() for i in range(10)), 1))
        mock.assert_has_awaits([asynctest.call(i) for i in range(1, 11)])


if __name__ == '__main__':
    asynctest.main()

