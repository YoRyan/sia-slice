import os
import re

import asynctest

import siaslice as ss


ENDPOINT = ss.SiadEndpoint(
        domain='http://localhost:9980', api_password=os.environ['SIAD_API'])


class UploadAndDownloadAndDelete(asynctest.TestCase):
    pass


class TestSiadOperations(asynctest.TestCase):

    async def test_delete_block(self):
        # Upload mock block 69.
        await ss.siad_post(ENDPOINT, b'', 'renter', 'uploadstream',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.69.x.lz')
        await ss.siad_post(ENDPOINT, b'', 'renter', 'uploadstream',
                           'siaslice_test_dir_abcd1234', 'siaslice.1MiB.69.x.lz')

        # Delete block 69.
        await ss.siapath_delete_block(
                ENDPOINT, ('siaslice_test_dir_abcd1234',), 69)

        # Check for empty directory.
        response = await ss.siad_json(await ss.siad_get(
                ENDPOINT, 'renter', 'dir', 'siaslice_test_dir_abcd1234'))
        self.assertEqual(response['files'], [])

        # Clean up test directory.
        await ss.siad_post(ENDPOINT, b'', 'renter', 'dir',
                           'siaslice_test_dir_abcd1234', action='delete')


if __name__ == '__main__':
    asynctest.main()

