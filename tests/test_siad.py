from os import environ

import asynctest

import siaslice as ss


DOMAIN = 'http://localhost:9980'
API_PASS = environ['SIA_API_PASSWORD']


class GetRequest(asynctest.TestCase):

    async def setUp(self):
        self.session = ss.SiadSession(DOMAIN, API_PASS)
        await self.session.create()

    async def tearDown(self):
        await self.session.close()

    async def test_version_check(self):
        response = await self.session.get('daemon', 'version')
        self.assertEqual(response.status, 200)
        self.assertIn('version', await response.json())

    async def test_stream_nonexistent_file(self):
        with self.assertRaises(ss.SiadError) as err:
            await self.session.get('renter', 'stream', 'siaslice_test_file_abcd1234')
            self.assertEqual(err.status, 500)


class PostRequest(asynctest.TestCase):

    async def setUp(self):
        self.session = ss.SiadSession(DOMAIN, API_PASS)
        await self.session.create()

    async def tearDown(self):
        await self.session.close()

    async def test_valid_siapath(self):
        response = await self.session.post(b'', 'renter', 'validatesiapath',
                                           'this', 'is', 'a', 'valid', '$iapath')
        self.assertEqual(response.status, 204)
        self.assertEqual(await response.text(), '')

    async def test_invalid_siapath(self):
        with self.assertRaises(ss.SiadError) as err:
            await self.session.post(b'', 'renter', 'validatesiapath', '')
            self.assertEqual(err.status, 400)


if __name__ == '__main__':
    asynctest.main()

