import asynctest

import siaslice as ss


class TestTaskGenerator(asynctest.TestCase):

    async def test_sync_generator(self):
        mock = asynctest.CoroutineMock()
        async def cor():
            await mock(1)
        generator = (cor() for i in range(10))
        await ss.run_all_tasks(generator, max_concurrent=2)
        mock.assert_has_awaits([asynctest.call(1)]*10)


if __name__ == '__main__':
    asynctest.main()

