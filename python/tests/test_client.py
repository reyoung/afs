from __future__ import annotations

import unittest

from afs_sdk import AfsClient, ExecuteInput


class ExecuteInputEnvTests(unittest.IsolatedAsyncioTestCase):
    async def test_iter_raw_requests_accepts_mapping_env(self) -> None:
        client = AfsClient("127.0.0.1", 1)
        try:
            reqs = client._iter_raw_requests(
                ExecuteInput(
                    image="alpine",
                    command=["/bin/sh", "-lc", "env"],
                    env={"FOO": "bar", "PATH": "/custom/bin"},
                )
            )

            first = await anext(reqs)

            self.assertEqual(first.start.image, "alpine")
            self.assertEqual(list(first.start.command), ["/bin/sh", "-lc", "env"])
            self.assertEqual(list(first.start.env), ["FOO=bar", "PATH=/custom/bin"])
        finally:
            await client.close()

    async def test_iter_raw_requests_rejects_non_string_env_entries(self) -> None:
        client = AfsClient("127.0.0.1", 1)
        try:
            reqs = client._iter_raw_requests(
                ExecuteInput(
                    image="alpine",
                    command=["/bin/true"],
                    env=["FOO=bar", 123],  # type: ignore[list-item]
                )
            )

            with self.assertRaises(TypeError):
                await anext(reqs)
        finally:
            await client.close()


if __name__ == "__main__":
    unittest.main()
