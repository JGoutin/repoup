"""Test S3 storage."""
from os import urandom
from os.path import isfile


async def test_s3_storage() -> None:
    """Test S3 storage.

    This is also a sanity check of the S3 mock.
    """
    from repoup.exceptions import PackageNotFound
    from repoup.storage import get_storage

    async with get_storage("s3://bucket/repo") as storage:
        # Put and get object
        content = urandom(64)
        key = "object"
        await storage.put_object(key, content)
        assert (await storage.get_object(key)) == content

        # Put and get file
        content = urandom(64)
        key = "file"
        path = storage.tmp_join("file")
        assert path.startswith(storage.path)

        with open(path, "wb") as file:
            file.write(content)
        await storage.put_file(key)

        assert isfile(path)
        await storage.remove_tmp("file")
        assert not isfile(path)
        await storage.remove_tmp("file")  # Should not raise

        await storage.get_file(key)
        with open(path, "rb") as file:
            assert file.read() == content

        # Remove object
        assert await storage.exists(key)
        await storage.remove(key)
        assert not (await storage.exists(key))
        try:
            await storage.get_object(key)
        except PackageNotFound as error:
            assert error.status == 404
            assert key in error.message
        await storage.remove(key)  # Should not raise

        # Join path
        assert storage.join("key") == "repo/key"
        assert storage.join("key", absolute=True) == "key"
