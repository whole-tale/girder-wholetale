import shutil
from pathlib import Path
from typing import Optional, Union

from girder.constants import AccessType
from girder.models.folder import Folder
from girder.models.token import Token
from girder.models.user import User
from girder_jobs.models.job import Job
from girder_worker.girder_plugin.celery import getCeleryApp
from gwvolman.tasks import check_on_run, cleanup_run

from ..constants import FIELD_STATUS_CODE, PluginSettings, RunState, RunStatus
from ..utils import get_tale_dir_root
from .hierarchy import AbstractHierarchyModel
from .tale import Tale
from .version_hierarchy import VersionHierarchyModel


class RunHierarchyModel(AbstractHierarchyModel):
    root_tale_field = "runsRootId"

    def getStatus(self, rfolder: dict) -> dict:
        if FIELD_STATUS_CODE in rfolder:
            rs = RunStatus.get(rfolder[FIELD_STATUS_CODE])
        else:
            rs = RunStatus.UNKNOWN
        return {"status": rs.code, "statusString": rs.name}

    def setStatus(self, rfolder: dict, status: Union[int, RunState]) -> None:
        # TODO: add heartbeats (runs must regularly update status, otherwise they are considered
        # failed)
        if isinstance(status, int):
            _status = RunState.ALL[status]
        else:
            _status = status
        rfolder[FIELD_STATUS_CODE] = _status.code
        Folder().save(rfolder)
        runDir = Path(rfolder["fsPath"])
        self.write_status(runDir, _status)

    def create(
        self, version: dict, name: Optional[str], user: dict, allowRename: bool = False
    ) -> dict:
        if not name:
            name = self.generateName()

        versionsRoot = Folder().load(
            version["parentId"], user=user, level=AccessType.WRITE
        )
        taleId = versionsRoot["taleId"]
        tale = Tale().load(taleId, user=user, level=AccessType.WRITE)
        root = self.getRootFromTale(tale, user=user, level=AccessType.WRITE)
        name = self.checkNameSanity(name, root, allow_rename=allowRename)

        rootDir = get_tale_dir_root(tale, PluginSettings.RUNS_DIRS_ROOT)
        version_root_dir = get_tale_dir_root(tale, PluginSettings.VERSIONS_DIRS_ROOT)
        runFolder = self.createSubdir(rootDir, root, name, user=user)

        runFolder["runVersionId"] = version["_id"]
        runFolder[FIELD_STATUS_CODE] = RunStatus.UNKNOWN.code
        Folder().save(runFolder, False)

        # Structure is:
        #  @version -> ../Versions/<version> (link handled manually by FS)
        #  @workspace -> version/workspace (same)
        #  .status
        #  .stdout (created using stream() above)
        #  .stderr (-''-)
        runDir = Path(runFolder["fsPath"])
        (runDir / "version").symlink_to(version_root_dir / str(version["_id"]), True)
        (runDir / "workspace").mkdir()
        self.snapshotRecursive(
            None, (runDir / "version" / "workspace"), (runDir / "workspace")
        )
        self.write_status(runDir, RunStatus.UNKNOWN)

        Tale().updateTale(tale)
        VersionHierarchyModel().incrementReferenceCount(version)

        return runFolder

    @staticmethod
    def write_status(runDir: Path, status: RunState):
        with open(runDir / ".status", "w") as f:
            f.write("%s %s" % (status.code, status.name))

    def remove(self, rfolder: dict, user: dict) -> None:
        path = Path(rfolder["fsPath"])
        trashDir = path.parent / ".trash"
        version = Folder().load(
            rfolder["runVersionId"], level=AccessType.WRITE, user=user
        )
        Folder().remove(rfolder)
        shutil.move(path.as_posix(), trashDir)
        VersionHierarchyModel().decrementReferenceCount(version)

    def run_heartbeat(self, event):
        celery_inspector = getCeleryApp().control.inspect()
        try:
            active_queues = list(celery_inspector.active_queues().keys())
        except AttributeError:  # everything is dead
            active_queues = []
        active_runs = Folder().find(
            {
                FIELD_STATUS_CODE: {
                    "$in": [RunStatus.RUNNING.code, RunStatus.UNKNOWN.code]
                },
                "meta.container_name": {"$exists": True},
            }
        )
        for run in active_runs:
            queue = f"celery@{run['meta']['node_id']}"
            if queue not in active_queues:
                if run[FIELD_STATUS_CODE] == RunStatus.RUNNING.code:
                    # worker is presumed dead so we set run's status to UNK to reap it
                    # when it's back online.
                    self.setStatus(run, RunStatus.UNKNOWN)
                continue

            active_tasks = {task["id"] for task in celery_inspector.active()[queue]}
            run_job = Job().load(run["meta"]["jobId"], force=True)
            run_task_id = run_job["celeryTaskId"]

            # Task is gone, cleanup
            delete = run_task_id not in active_tasks

            if not delete:
                is_running = check_on_run.signature(
                    args=[run["meta"]], queue=run["meta"]["node_id"]
                ).apply_async()
                delete = not is_running.get(timeout=60)

            if delete:
                user = User().load(run["creatorId"], force=True)
                girder_token = Token().createToken(user=user, days=0.1)
                cleanup_run.signature(
                    args=[str(run["_id"])],
                    girder_client_token=str(girder_token["_id"]),
                    queue=run["meta"]["node_id"],
                ).apply_async()
