from __future__ import annotations

import argparse
import asyncio

from afs_sdk import AfsClient, ReconcileImageInput


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", default="127.0.0.1:62051", help="afs_proxy address")
    parser.add_argument("--image", required=True, help="image name")
    parser.add_argument("--tag", default="", help="image tag")
    parser.add_argument("--platform-os", default="linux")
    parser.add_argument("--platform-arch", default="amd64")
    parser.add_argument("--platform-variant", default="")
    parser.add_argument("--replica", type=int, required=True, help="target replica, must be >= 0")
    args = parser.parse_args()

    host, port_str = args.addr.rsplit(":", 1)
    port = int(port_str)
    req = ReconcileImageInput(
        image=args.image,
        tag=args.tag,
        platform_os=args.platform_os,
        platform_arch=args.platform_arch,
        platform_variant=args.platform_variant,
        replica=args.replica,
    )

    async with AfsClient(host, port) as client:
        resp = await client.reconcile_image(req)
        print(
            f"image_key={resp.image_key} current_replica={resp.current_replica} "
            f"requested_replica={resp.requested_replica} ensured={resp.ensured}"
        )


if __name__ == "__main__":
    asyncio.run(main())
