"""Independent native LXMF endpoint and Reticulum service for local qualification.

This helper does not import MEDRE. Its UTF-8 content/title extraction follows
the ordinary LXMF fields consumed by Reticulum MeshChat and similar clients.
"""

from __future__ import annotations

import json
import sys
import threading
from pathlib import Path


def main() -> None:
    """Exchange plain LXMF messages through an existing local network service."""
    import LXMF
    import RNS

    role, config_dir, storage = sys.argv[1:]
    reticulum = RNS.Reticulum(config_dir, loglevel=RNS.LOG_NONE)
    output_lock = threading.Lock()

    def emit(**event):
        with output_lock:
            print(json.dumps(event, ensure_ascii=False), flush=True)

    if role == "service":
        emit(event="ready", role="service", shared=reticulum.is_shared_instance)
        sys.stdin.readline()
        return

    root = Path(storage)
    root.mkdir()
    identity = RNS.Identity()
    router = LXMF.LXMRouter(identity=identity, storagepath=str(root))
    destination = router.register_delivery_identity(identity, stamp_cost=0)

    def delivered(message):
        emit(
            event="received",
            content=message.content.decode("utf-8"),
            title=message.title.decode("utf-8"),
        )

    class Announces:
        aspect_filter = "lxmf.delivery"

        def received_announce(self, destination_hash, announced_identity, app_data):
            emit(
                event="announce",
                address=destination_hash.hex(),
                display_name=LXMF.display_name_from_app_data(app_data),
            )

    router.register_delivery_callback(delivered)
    RNS.Transport.register_announce_handler(Announces())
    emit(
        event="ready",
        role="peer",
        address=destination.hash.hex(),
        attached=reticulum.is_connected_to_shared_instance,
    )
    last_message = None
    for line in sys.stdin:
        request = json.loads(line)
        if request["command"] == "stop":
            break
        if request["command"] == "announce":
            router.announce(destination.hash)
            emit(event="announced")
        elif request["command"] == "status":
            # Inspect only this campaign's last submission; never alter the
            # cached link or routing state whose recovery is being qualified.
            target = last_message.get_destination().hash if last_message else None
            direct = router.direct_links.get(target)
            backchannel = router.backchannel_links.get(target)
            emit(
                event="status",
                token=request["token"],
                message_state=last_message.state if last_message else None,
                delivery_attempts=last_message.delivery_attempts if last_message else 0,
                direct_link_status=direct.status if direct else None,
                backchannel_link_status=backchannel.status if backchannel else None,
                path_known=RNS.Transport.has_path(target) if target else None,
                attached=reticulum.is_connected_to_shared_instance,
            )
        elif request["command"] == "send":
            target = bytes.fromhex(request["destination"])
            remote = RNS.Identity.recall(target)
            if remote is None:
                raise RuntimeError("peer destination identity is not known")
            target_destination = RNS.Destination(
                remote, RNS.Destination.OUT, RNS.Destination.SINGLE, "lxmf", "delivery"
            )
            message = LXMF.LXMessage(
                target_destination,
                destination,
                request["content"],
                desired_method=LXMF.LXMessage.DIRECT,
            )
            last_message = message
            router.handle_outbound(message)
            emit(event="submitted")


if __name__ == "__main__":
    main()
