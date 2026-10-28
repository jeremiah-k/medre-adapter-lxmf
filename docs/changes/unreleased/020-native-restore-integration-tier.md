# Qualify shared-Reticulum restore in the local integration tier

Classify the service/peer/worker restore campaign as both SDK and local
integration. Keep it out of the general SDK API-contract selection and retain
both restore directions in the required integration selection. Run standalone
local integration on Python 3.13 and 3.14 so the failing interpreter remains
covered. Collection uses pytest's supported multi-mark list.

Delivery assertions, twenty-second deadlines, and uncertain-send fences remain
intact. Test classification does not establish native restore reliability.
