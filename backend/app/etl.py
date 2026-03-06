"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select

from app.settings import settings
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


import httpx


env = None

async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    A simple HTTP GET is performed using :class:`httpx.AsyncClient`. The
    credentials stored in ``settings`` are sent using standard HTTP Basic
    authentication. ``raise_for_status`` ensures we propagate any non-200
    response back to the caller as an exception; callers may catch this
    if they want to retry.
    """

    url = f"{settings.autochecker_api_url.rstrip('/')}/api/items"
    auth = (settings.autochecker_email, settings.autochecker_password)

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url, auth=auth)
        response.raise_for_status()
        # return the parsed JSON body; the API returns a plain list
        return response.json()


from datetime import timezone


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    Logs are returned in pages of up to ``limit`` entries. We repeatedly
    query until the ``has_more`` flag is ``False``. For incremental sync we
    optionally pass a ``since`` timestamp parameter; after each page we
    advance ``since`` to the ``submitted_at`` value of the last record to
    avoid duplicating work.

    The returned list contains raw log dicts exactly as the API produced
    them. Any non-200 status raises via :meth:`httpx.Response.raise_for_status`.
    """

    url = f"{settings.autochecker_api_url.rstrip('/')}/api/logs"
    auth = (settings.autochecker_email, settings.autochecker_password)

    all_logs: list[dict] = []
    params: dict[str, str | int] = {"limit": 100}

    if since is not None:
        # API expects ISO timestamp with Z suffix
        if since.tzinfo is None:
            params["since"] = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            params["since"] = since.astimezone(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            response = await client.get(url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()
            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more"):
                break

            # prepare next request using last log's submitted_at
            last = logs[-1]
            params["since"] = last["submitted_at"]

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


from sqlmodel import select


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    The algorithm is two‑phase: first discover or create all lab entries,
    then handle task children once we know each lab's database primary key.
    We keep a ``lab_map`` from the short ID provided by the API to the
    corresponding ``ItemRecord`` instance (either newly created or already
    existing) so that when processing tasks we can quickly look up the
    ``parent_id``.

    A single ``session.commit`` is performed at the end; we call
    ``session.flush`` when we add a new record to ensure its ``id`` is
    populated for use by subsequent inserts.
    """

    from app.models.item import ItemRecord

    created = 0
    lab_map: dict[str, ItemRecord] = {}

    # Phase 1: labs
    for entry in items:
        if entry.get("type") != "lab":
            continue
        lab_key = entry.get("lab")
        title = entry.get("title")
        # look for existing record
        stmt = select(ItemRecord).where(
            ItemRecord.type == "lab", ItemRecord.title == title
        )
        result = await session.exec(stmt)
        existing = result.first()
        if existing is None:
            new_lab = ItemRecord(type="lab", title=title)
            session.add(new_lab)
            await session.flush()  # assign PK so tasks can reference it
            lab_map[lab_key] = new_lab
            created += 1
        else:
            lab_map[lab_key] = existing

    # Phase 2: tasks
    for entry in items:
        if entry.get("type") != "task":
            continue
        lab_key = entry.get("lab")
        parent = lab_map.get(lab_key)
        if parent is None:
            # weird: task without a lab, skip
            continue
        title = entry.get("title")
        stmt = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == title,
            ItemRecord.parent_id == parent.id,
        )
        result = await session.exec(stmt)
        existing = result.first()
        if existing is None:
            new_task = ItemRecord(type="task", title=title, parent_id=parent.id)
            session.add(new_task)
            created += 1

    await session.commit()
    return created


from sqlmodel import select


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Implements an idempotent import of raw logs. We first construct a
    lookup table from the raw item catalog so that we can translate the
    ``lab``/``task`` short IDs that appear in each log into the human-\
    readable titles stored in the ``item`` table.  The database queries are
    intentionally simple to keep the function easy to follow.
    """

    from app.models.learner import Learner
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord

    # build title lookup from catalog
    title_lookup: dict[tuple[str, str | None], str] = {}
    for entry in items_catalog:
        key = (entry.get("lab"), entry.get("task"))
        title_lookup[key] = entry.get("title")

    created = 0

    for log in logs:
        # --- learner upsert ------------------------------------------------
        ext_id = log.get("student_id")
        stmt = select(Learner).where(Learner.external_id == ext_id)
        result = await session.exec(stmt)
        learner = result.first()
        if learner is None:
            learner = Learner(
                external_id=ext_id, student_group=log.get("group", "")
            )
            session.add(learner)
            await session.flush()

        # --- find item ------------------------------------------------------
        key = (log.get("lab"), log.get("task"))
        title = title_lookup.get(key)
        if title is None:
            # unknown item (maybe a lab without tasks or vice versa)
            continue
        stmt = select(ItemRecord).where(ItemRecord.title == title)
        result = await session.exec(stmt)
        item = result.first()
        if item is None:
            continue  # catalog had title but item not yet inserted

        # --- idempotency check ------------------------------------------------
        stmt = select(InteractionLog).where(
            InteractionLog.external_id == log.get("id")
        )
        result = await session.exec(stmt)
        if result.first():
            continue

        # --- insert interaction ---------------------------------------------
        submitted = log.get("submitted_at")
        if submitted:
            created_at = datetime.fromisoformat(submitted.replace("Z", ""))
        else:
            created_at = datetime.utcnow()

        interaction = InteractionLog(
            external_id=log.get("id"),
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at,
        )
        session.add(interaction)
        created += 1

    await session.commit()
    return created


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


from sqlmodel import select, desc


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    This function glues together the extract and load helpers defined
    earlier. It is intentionally concise since the heavy lifting is
    performed by the other helpers.
    """

    # --- items -------------------------------------------------------------
    items = await fetch_items()
    await load_items(items, session)

    # --- determine last synced timestamp ----------------------------------
    from app.models.interaction import InteractionLog

    stmt = (
        select(InteractionLog.created_at)
        .order_by(desc(InteractionLog.created_at))
        .limit(1)
    )
    result = await session.exec(stmt)
    # ``first()`` returns the scalar value when selecting a column
    last = result.first()
    since = last

    # --- fetch and load logs ----------------------------------------------
    logs = await fetch_logs(since=since)
    new_count = await load_logs(logs, items, session)

    # --- compute total interactions ---------------------------------------
    stmt2 = select(InteractionLog)
    result2 = await session.exec(stmt2)
    total = len(result2.all())

    return {"new_records": new_count, "total_records": total}
