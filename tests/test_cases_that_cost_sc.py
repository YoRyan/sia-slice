import lzma
import os
import re

import asynctest
from aiofile import AIOFile

import siaslice as ss


ENDPOINT = ss.SiadEndpoint(
        domain='http://localhost:9980', api_password=os.environ['SIAD_API'])


class UploadAndDownloadAndDelete(asynctest.TestCase):
    pass


class TestSiadOperations(asynctest.TestCase):

    async def test_delete_block(self):
        await ss.siad_post(ENDPOINT, b'', 'renter', 'uploadstream',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.69.x.lz')
        await ss.siad_post(ENDPOINT, b'', 'renter', 'uploadstream',
                           'siaslice_test_dir_abcd1234', 'siaslice.1MiB.69.x.lz')

        await ss.siapath_delete_block(
                ENDPOINT, ('siaslice_test_dir_abcd1234',), 69)

        response = await ss.siad_json(await ss.siad_get(
                ENDPOINT, 'renter', 'dir', 'siaslice_test_dir_abcd1234'))
        self.assertEqual(response['files'], [])

        await ss.siad_post(ENDPOINT, b'', 'renter', 'dir',
                           'siaslice_test_dir_abcd1234', action='delete')

    async def test_read_block_map(self):
        await ss.siad_post(ENDPOINT, b'', 'renter', 'uploadstream',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.0.x.lz')
        await ss.siad_post(ENDPOINT, b'', 'renter', 'uploadstream',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.1.y.lz')
        await ss.siad_post(ENDPOINT, b'', 'renter', 'uploadstream',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.2.z.lz')

        block_map = await ss.siapath_block_map(ENDPOINT,
                                               ('siaslice_test_dir_abcd1234',))
        self.assertEqual(block_map.md5_hashes, ['x', 'y', 'z'])

        await ss.siad_post(ENDPOINT, b'', 'renter', 'dir',
                           'siaslice_test_dir_abcd1234', action='delete')

    async def test_sia_mirror_1_block(self):
        prior_map = ss.BlockMap(block_size=40*1000*1000, md5_hashes=[])
        async with AIOFile('40MiB.img', 'rb') as source_afp:
            reference_bytes = await source_afp.read()
            await ss.siapath_mirror(ENDPOINT, source_afp,
                                    ('siaslice_test_dir_abcd1234',), prior_map)

        uploaded_bytes = b''
        async for chunk in ss.siad_stream_lz(
                ENDPOINT, 'siaslice_test_dir_abcd1234',
                'siaslice.40MiB.0.48e9a108a3ec623652e7988af2f88867.lz'):
            uploaded_bytes += chunk
        self.assertEqual(uploaded_bytes, reference_bytes)

        await ss.siad_post(ENDPOINT, b'', 'renter', 'dir',
                           'siaslice_test_dir_abcd1234', action='delete')

    async def test_sia_mirror_2_blocks(self):
        prior_map = ss.BlockMap(block_size=20*1000*1000, md5_hashes=[])
        async with AIOFile('40MiB.img', 'rb') as source_afp:
            reference_bytes = await source_afp.read()
            await ss.siapath_mirror(ENDPOINT, source_afp,
                                    ('siaslice_test_dir_abcd1234',), prior_map)

        uploaded_bytes = b''
        async for chunk in ss.siad_stream_lz(
                ENDPOINT, 'siaslice_test_dir_abcd1234',
                'siaslice.20MiB.0.10e4462c9d0b08e7f0b304c4fbfeafa3.lz'):
            uploaded_bytes += chunk
        async for chunk in ss.siad_stream_lz(
                ENDPOINT, 'siaslice_test_dir_abcd1234',
                'siaslice.20MiB.1.10e4462c9d0b08e7f0b304c4fbfeafa3.lz'):
            uploaded_bytes += chunk
        self.assertEqual(uploaded_bytes, reference_bytes)

        await ss.siad_post(ENDPOINT, b'', 'renter', 'dir',
                           'siaslice_test_dir_abcd1234', action='delete')


if __name__ == '__main__':
    asynctest.main()

