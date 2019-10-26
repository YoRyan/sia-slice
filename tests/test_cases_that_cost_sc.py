import os

import asynctest
from aiofile import AIOFile

import siaslice as ss


DOMAIN = 'http://localhost:9980'
API_PASS = os.environ['SIA_API_PASSWORD']


class TestSiaOperations(asynctest.TestCase):
    TEST_DIR = 'siaslice_test_dir_abcd1234'

    async def setUp(self):
        self.session = ss.SiadSession(DOMAIN, API_PASS)
        await self.session.open()

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
        async with AIOFile('40MiBempty.img', mode='rb') as afp:
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
        async with AIOFile('40MiBempty.img', mode='rb') as afp:
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
        storage = ss.SiapathStorage(self.session, TestSiaOperations.TEST_DIR,
                                    default_block_size=40*1000*1000)
        await storage.update()
        async with AIOFile('40MiBempty.img', mode='rb') as afp:
            reference = await afp.read()
            async for status in ss.siapath_mirror(storage, afp):
                pass

        async with AIOFile('test_download.img', 'wb') as afp:
            async for status in ss.siapath_download(storage, afp):
                pass
        with open('test_download.img', 'rb') as fp:
            downloaded = fp.read()
        os.remove('test_download.img')
        self.assertEqual(downloaded, reference)

        await storage.delete(0)

    async def test_sia_download_2_blocks(self):
        storage = ss.SiapathStorage(self.session, TestSiaOperations.TEST_DIR,
                                    default_block_size=20*1000*1000)
        await storage.update()
        async with AIOFile('40MiBempty.img', mode='rb') as afp:
            reference = await afp.read()
            async for status in ss.siapath_mirror(storage, afp):
                pass

        async with AIOFile('test_download.img', 'wb') as afp:
            async for status in ss.siapath_download(storage, afp):
                pass
        with open('test_download.img', 'rb') as fp:
            downloaded = fp.read()
        os.remove('test_download.img')
        self.assertEqual(downloaded, reference)

        await storage.delete(0)
        await storage.delete(1)


if __name__ == '__main__':
    asynctest.main()

