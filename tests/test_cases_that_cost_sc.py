import os
from lzma import compress

import asynctest
from aiofile import AIOFile

import siaslice as ss


DOMAIN = 'http://localhost:9980'
API_PASS = os.environ['SIAD_API']


class TestSiaOperations(asynctest.TestCase):

    async def setUp(self):
        self.session = ss.SiadSession(DOMAIN, API_PASS)
        await self.session.create()

    async def tearDown(self):
        await self.session.close()

    async def test_delete_block(self):
        await ss.siad_post(self.session, b'', 'renter', 'uploadstream',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.69.x.lz')
        await ss.siad_post(self.session, b'', 'renter', 'uploadstream',
                           'siaslice_test_dir_abcd1234', 'siaslice.1MiB.69.x.lz')

        await ss.siapath_delete_block(
                self.session, ['siaslice_test_dir_abcd1234'], 69)

        response = await ss.siad_json(await ss.siad_get(
                self.session, 'renter', 'dir', 'siaslice_test_dir_abcd1234'))
        self.assertEqual(response['files'], [])

        await ss.siad_post(self.session, b'', 'renter', 'delete',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.69.x.lz')
        await ss.siad_post(self.session, b'', 'renter', 'delete',
                           'siaslice_test_dir_abcd1234', 'siaslice.1MiB.69.x.lz')

    async def test_read_block_map(self):
        await ss.siad_post(self.session, b'', 'renter', 'uploadstream',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.0.x.lz')
        await ss.siad_post(self.session, b'', 'renter', 'uploadstream',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.1.y.lz')
        await ss.siad_post(self.session, b'', 'renter', 'uploadstream',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.2.z.lz')

        block_map = await ss.siapath_block_map(self.session,
                                               ['siaslice_test_dir_abcd1234'])
        self.assertEqual(block_map.md5_hashes, ['x', 'y', 'z'])

        await ss.siad_post(self.session, b'', 'renter', 'delete',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.0.x.lz')
        await ss.siad_post(self.session, b'', 'renter', 'delete',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.1.y.lz')
        await ss.siad_post(self.session, b'', 'renter', 'delete',
                           'siaslice_test_dir_abcd1234', 'siaslice.40MiB.2.z.lz')

    async def test_sia_mirror_1_block(self):
        prior_map = ss.BlockMap(block_size=40*1000*1000, md5_hashes=[])
        async with AIOFile('40MiBempty.img', 'rb') as source_afp:
            reference_bytes = await source_afp.read()
            async for status in ss.siapath_mirror(
                    self.session, source_afp,
                    ['siaslice_test_dir_abcd1234'], prior_map):
                pass

        uploaded_bytes = b''
        async for chunk in ss.siad_stream_lz(
                self.session, 'siaslice_test_dir_abcd1234',
                'siaslice.40MiB.0.48e9a108a3ec623652e7988af2f88867.lz'):
            uploaded_bytes += chunk
        self.assertEqual(uploaded_bytes, reference_bytes)

        await ss.siad_post(
                self.session, b'', 'renter', 'delete', 'siaslice_test_dir_abcd1234',
                'siaslice.40MiB.0.48e9a108a3ec623652e7988af2f88867.lz')

    async def test_sia_mirror_2_blocks(self):
        prior_map = ss.BlockMap(block_size=20*1000*1000, md5_hashes=[])
        async with AIOFile('40MiBempty.img', 'rb') as source_afp:
            reference_bytes = await source_afp.read()
            async for status in ss.siapath_mirror(
                    self.session, source_afp,
                    ['siaslice_test_dir_abcd1234'], prior_map):
                pass

        uploaded_bytes = b''
        async for chunk in ss.siad_stream_lz(
                self.session, 'siaslice_test_dir_abcd1234',
                'siaslice.20MiB.0.10e4462c9d0b08e7f0b304c4fbfeafa3.lz'):
            uploaded_bytes += chunk
        async for chunk in ss.siad_stream_lz(
                self.session, 'siaslice_test_dir_abcd1234',
                'siaslice.20MiB.1.10e4462c9d0b08e7f0b304c4fbfeafa3.lz'):
            uploaded_bytes += chunk
        self.assertEqual(uploaded_bytes, reference_bytes)

        await ss.siad_post(
                self.session, b'', 'renter', 'delete', 'siaslice_test_dir_abcd1234',
                'siaslice.20MiB.0.10e4462c9d0b08e7f0b304c4fbfeafa3.lz')
        await ss.siad_post(
                self.session, b'', 'renter', 'delete', 'siaslice_test_dir_abcd1234',
                'siaslice.20MiB.1.10e4462c9d0b08e7f0b304c4fbfeafa3.lz')

    async def test_sia_download_1_block(self):
        prior_map = ss.BlockMap(block_size=40*1000*1000, md5_hashes=[])
        async with AIOFile('40MiBempty.img', mode='rb') as afp:
            reference_bytes = await afp.read()
            async for status in ss.siapath_mirror(
                    self.session, afp, ['siaslice_test_dir_abcd1234'], prior_map):
                pass

        async with AIOFile('test_download.img', 'wb') as afp:
            async for status in ss.siapath_download(
                    self.session, afp, ['siaslice_test_dir_abcd1234']):
                pass
        with open('test_download.img', 'rb') as fp:
            download_bytes = fp.read()
        os.remove('test_download.img')

        self.assertEqual(download_bytes, reference_bytes)

        await ss.siad_post(
                self.session, b'', 'renter', 'delete', 'siaslice_test_dir_abcd1234',
                'siaslice.40MiB.0.48e9a108a3ec623652e7988af2f88867.lz')

    async def test_sia_download_2_blocks(self):
        prior_map = ss.BlockMap(block_size=20*1000*1000, md5_hashes=[])
        async with AIOFile('40MiBempty.img', mode='rb') as afp:
            reference_bytes = await afp.read()
            async for status in ss.siapath_mirror(
                    self.session, afp, ['siaslice_test_dir_abcd1234'], prior_map):
                pass

        async with AIOFile('test_download.img', 'wb') as afp:
            async for status in ss.siapath_download(
                    self.session, afp, ['siaslice_test_dir_abcd1234']):
                pass
        with open('test_download.img', 'rb') as fp:
            download_bytes = fp.read()
        os.remove('test_download.img')

        self.assertEqual(download_bytes, reference_bytes)

        await ss.siad_post(
                self.session, b'', 'renter', 'delete', 'siaslice_test_dir_abcd1234',
                'siaslice.20MiB.0.10e4462c9d0b08e7f0b304c4fbfeafa3.lz')
        await ss.siad_post(
                self.session, b'', 'renter', 'delete', 'siaslice_test_dir_abcd1234',
                'siaslice.20MiB.1.10e4462c9d0b08e7f0b304c4fbfeafa3.lz')


if __name__ == '__main__':
    asynctest.main()

