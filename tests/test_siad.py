import os
import re

import asynctest

from siaslice import SiadEndpoint, siad_get, siad_post


ENDPOINT = SiadEndpoint(
        domain='http://localhost:9980', api_password=os.environ['SIAD_API'])


class GetRequest(asynctest.TestCase):

    async def test_version_check(self):
        response = await siad_get(ENDPOINT, 'daemon', 'version')
        self.assertEqual(response.status, 200)
        self.assertIn('version', await response.json())

    async def test_stream_nonexistent_file(self):
        response = await siad_get(ENDPOINT, 'renter', 'stream',
                                  'siaslice_test_file_abcd1234')
        self.assertEqual(response.status, 500)
        self.assertIn('message', await response.json())


class PostRequest(asynctest.TestCase):

    async def test_valid_siapath(self):
        response = await siad_post(ENDPOINT, b'', 'renter', 'validatesiapath',
                                   'this', 'is', 'a', 'valid', '$iapath')
        self.assertEqual(response.status, 204)
        self.assertEqual(await response.text(), '')

    async def test_invalid_siapath(self):
        response = await siad_post(
                ENDPOINT, b'', 'renter', 'validatesiapath', '')
        self.assertEqual(response.status, 400)
        self.assertIn('message', await response.json())


if __name__ == '__main__':
    asynctest.main()

