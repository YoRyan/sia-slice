import os
from lzma import compress

import asynctest
from aiofile import AIOFile

import siaslice as ss


DOMAIN = 'http://localhost:9980'
API_PASS = os.environ['SIAD_API']


class TestSiaOperations(asynctest.TestCase):
    TEST_DIR = 'siaslice_test_dir_abcd1234'

    async def setUp(self):
        self.session = ss.SiadSession(DOMAIN, API_PASS)
        await self.session.create()

    async def tearDown(self):
        await self.session.close()

    async def test_delete_block(self):
        await self.session.post(
                b'', 'renter', 'uploadstream',
                TestSiaOperations.TEST_DIR, 'siaslice.40MiB.69.x.lz')
        storage = ss.SiapathStorage(self.session, TestSiaOperations.TEST_DIR)
        await storage.update()

        await storage.delete(69)
        self.assertNotIn(69, storage.block_files)

    async def test_read_hashes(self):
        await self.session.post(b'', 'renter', 'uploadstream',
                                TestSiaOperations.TEST_DIR, 'siaslice.40MiB.0.x.lz')
        await self.session.post(b'', 'renter', 'uploadstream',
                                TestSiaOperations.TEST_DIR, 'siaslice.40MiB.1.y.lz')
        await self.session.post(b'', 'renter', 'uploadstream',
                                TestSiaOperations.TEST_DIR, 'siaslice.40MiB.2.z.lz')
        storage = ss.SiapathStorage(self.session, TestSiaOperations.TEST_DIR)
        await storage.update()

        storage_hashes = [storage.block_files[i].md5_hash for i in range(3)]
        self.assertEqual(storage_hashes, ['x', 'y', 'z'])

        await storage.delete(0)
        await storage.delete(1)
        await storage.delete(2)

    async def test_sia_mirror_1_block(self):
        storage = ss.SiapathStorage(self.session, TestSiaOperations.TEST_DIR,
                                    default_block_size=40*1000*1000)
        await storage.update()
        async with AIOFile('40MiBempty.img', 'rb') as afp:
            reference = await afp.read()
            async for status in ss.siapath_mirror(storage, afp):
                pass

        uploaded = b''
        async for chunk in storage.download(0):
            uploaded += chunk
        self.assertEqual(uploaded, reference)

        await storage.delete(0)

    async def test_sia_mirror_2_blocks(self):
        storage = ss.SiapathStorage(self.session, TestSiaOperations.TEST_DIR,
                                    default_block_size=20*1000*1000)
        await storage.update()
        async with AIOFile('40MiBempty.img', 'rb') as afp:
            reference = await afp.read()
            async for status in ss.siapath_mirror(storage, afp):
                pass

        uploaded = b''
        async for chunk in storage.download(0):
            uploaded += chunk
        async for chunk in storage.download(1):
            uploaded += chunk
        self.assertEqual(uploaded, reference)

        await storage.delete(0)
        await storage.delete(1)

    async def test_sia_download_1_block(self):
        prior_map = ss.BlockMap(block_size=40*1000*1000, md5_hashes=[])
        async with AIOFile('40MiBempty.img', mode='rb') as afp:
            reference_bytes = await afp.read()
            async for status in ss.siapath_mirror(
                    self.session, afp, [TestSiaOperations.TEST_DIR], prior_map):
                pass

        async with AIOFile('test_download.img', 'wb') as afp:
            async for status in ss.siapath_download(
                    self.session, afp, [TestSiaOperations.TEST_DIR]):
                pass
        with open('test_download.img', 'rb') as fp:
            download_bytes = fp.read()
        os.remove('test_download.img')

        self.assertEqual(download_bytes, reference_bytes)

        await ss.siad_post(
                self.session, b'', 'renter', 'delete', TestSiaOperations.TEST_DIR,
                'siaslice.40MiB.0.48e9a108a3ec623652e7988af2f88867.lz')

    async def test_sia_download_2_blocks(self):
        prior_map = ss.BlockMap(block_size=20*1000*1000, md5_hashes=[])
        async with AIOFile('40MiBempty.img', mode='rb') as afp:
            reference_bytes = await afp.read()
            async for status in ss.siapath_mirror(
                    self.session, afp, [TestSiaOperations.TEST_DIR], prior_map):
                pass

        async with AIOFile('test_download.img', 'wb') as afp:
            async for status in ss.siapath_download(
                    self.session, afp, [TestSiaOperations.TEST_DIR]):
                pass
        with open('test_download.img', 'rb') as fp:
            download_bytes = fp.read()
        os.remove('test_download.img')

        self.assertEqual(download_bytes, reference_bytes)

        await ss.siad_post(
                self.session, b'', 'renter', 'delete', TestSiaOperations.TEST_DIR,
                'siaslice.20MiB.0.10e4462c9d0b08e7f0b304c4fbfeafa3.lz')
        await ss.siad_post(
                self.session, b'', 'renter', 'delete', TestSiaOperations.TEST_DIR,
                'siaslice.20MiB.1.10e4462c9d0b08e7f0b304c4fbfeafa3.lz')


if __name__ == '__main__':
    asynctest.main()

