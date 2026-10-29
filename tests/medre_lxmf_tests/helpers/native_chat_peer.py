"""Independent native LXMF endpoint and Reticulum service for local qualification.

This helper does not import MEDRE. Its UTF-8 content/title extraction follows
the ordinary LXMF fields consumed by Reticulum MeshChat and similar clients.
"""

from __future__ import annotations

import json
import sys
import threading
import time
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

    announce_seen = {}
    announce_lock = threading.Lock()

    class Announces:
        aspect_filter = "lxmf.delivery"

        def received_announce(self, destination_hash, announced_identity, app_data):
            with announce_lock:
                announce_seen[destination_hash.hex()] = time.monotonic()
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
    accepted_at = None
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
            with announce_lock:
                announce_at = announce_seen.get(target.hex()) if target else None
            announce_age = (
                max(0.0, time.monotonic() - announce_at)
                if announce_at is not None
                else None
            )
            emit(
                event="status",
                token=request["token"],
                message_state=last_message.state if last_message else None,
                delivery_attempts=last_message.delivery_attempts if last_message else 0,
                direct_link_status=direct.status if direct else None,
                direct_link_active=direct is not None
                and direct.status == RNS.Link.ACTIVE,
                backchannel_link_status=backchannel.status if backchannel else None,
                backchannel_link_active=(
                    backchannel is not None and backchannel.status == RNS.Link.ACTIVE
                ),
                path_known=RNS.Transport.has_path(target) if target else None,
                matching_announce_age_seconds=announce_age,
                attached=reticulum.is_connected_to_shared_instance,
                accepted_age_seconds=(
                    time.monotonic() - accepted_at if accepted_at is not None else None
                ),
                direct_link_age_seconds=(
                    max(0, time.time() - direct.request_time) if direct else None
                ),
                direct_link_timeout_seconds=(
                    direct.establishment_timeout if direct else None
                ),
                direct_link_expected_hops=direct.expected_hops if direct else None,
                retry_due_in_seconds=(
                    max(0, last_message.next_delivery_attempt - time.time())
                    if last_message and hasattr(last_message, "next_delivery_attempt")
                    else None
                ),
                outbound_processing_locked=router.outbound_processing_lock.locked(),
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
            accepted_at = time.monotonic()
            emit(event="submitted", token=request["token"])


if __name__ == "__main__":
    main()
