:orphan:

**Removed Features**

-  **Breaking Change** We removed the accidentally-exposed Session object from the
   ``determined.experimental.client`` namespace. It was never meant to be a public API and it was
   not documented in :ref:`python-sdk`, but it was exposed in that namespace nonetheless. It was
   also available as a deprecated legacy alias, ``determined.experimental.Session``. It is expected
   that most users use the Python SDK normally and are unaffected by this change.

-  **Breaking Change** Add a new requirement for runtime configurations that there be a writable
   ``$HOME`` directory in every container. Previously, there was extremely limited support for such
   containers, and that was merely by coincidence. The most likely situation under which this could
   affect a users is if jobs were configured to run as the ``nobody`` user inside a container,
   rather than the ``det-nobody`` alternative recommended by :ref:`run-unprivileged-tasks`. This may
   also affect users who are combining non-root tasks with custom images that are not based on one
   of Determined's official images. It is expected that few or no users are affected by this change.
