from os import environ

import asynctest

import siaslice as ss


DOMAIN = 'http://localhost:9980'
API_PASS = environ['SIAD_API']


class GetRequest(asynctest.TestCase):

    async def setUp(self):
        self.session = ss.SiadSession(DOMAIN, API_PASS)
        await self.session.create()

    async def tearDown(self):
        await self.session.close()

    async def test_version_check(self):
        response = await ss.siad_get(self.session, 'daemon', 'version')
        self.assertEqual(response.status, 200)
        self.assertIn('version', await response.json())

    async def test_stream_nonexistent_file(self):
        response = await ss.siad_get(self.session, 'renter', 'stream',
                                     'siaslice_test_file_abcd1234')
        self.assertEqual(response.status, 500)
        self.assertIn('message', await response.json())


class PostRequest(asynctest.TestCase):

    async def setUp(self):
        self.session = ss.SiadSession(DOMAIN, API_PASS)
        await self.session.create()

    async def tearDown(self):
        await self.session.close()

    async def test_valid_siapath(self):
        response = await ss.siad_post(
                self.session, b'', 'renter', 'validatesiapath',
                'this', 'is', 'a', 'valid', '$iapath')
        self.assertEqual(response.status, 204)
        self.assertEqual(await response.text(), '')

    async def test_invalid_siapath(self):
        response = await ss.siad_post(
                self.session, b'', 'renter', 'validatesiapath', '')
        self.assertEqual(response.status, 400)
        self.assertIn('message', await response.json())


if __name__ == '__main__':
    asynctest.main()

