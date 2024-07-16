import datetime
from typing import Union

from girder import events
from girder.api import access
from girder.api.describe import Description, autoDescribeRoute
from girder.api.rest import filtermodel
from girder.constants import AccessType, TokenScope
from girder.models.folder import Folder
from girder.models.token import Token
from girder_jobs.constants import JobStatus
from girder_jobs.models.job import Job
from gwvolman.constants import RECORDED_RUN_STEP_TOTAL
from gwvolman.tasks import recorded_run

from ..constants import FIELD_STATUS_CODE, RUNS_ROOT_DIR_NAME, RunState, RunStatus
from ..models.run_hierarchy import RunHierarchyModel
from ..models.tale import Tale
from ..utils import init_progress
from .abstract_resource import AbstractVRResource


class Run(AbstractVRResource):
    root_tale_field = "runsRootId"

    def __init__(self):
        super().__init__("run", RUNS_ROOT_DIR_NAME)
        self.route("PATCH", (":id", "status"), self.setStatus)
        self.route("GET", (":id", "status"), self.status)
        self.route("POST", (":id", "start"), self.startRun)
        events.bind("rest.put.run/:id.after", "wt_versioning", self.update_parents)
        events.bind("rest.delete.run/:id.before", "wt_versioning", self.update_parents)
        events.bind("jobs.job.update.after", "wt_versioning", self.updateRunStatus)
        self.model = RunHierarchyModel()
        events.bind("heartbeat", "wt_versioning", self.model.run_heartbeat)

    @access.public
    @filtermodel("folder")
    @autoDescribeRoute(
        Description("Retrieves the runs root folder for this tale.")
        .modelParam(
            "taleId",
            "The ID of a tale",
            model=Tale,
            level=AccessType.READ,
            paramType="query",
            destName="tale",
        )
        .errorResponse(
            "Access was denied (if current user does not have write access to this "
            "tale)",
            403,
        )
    )
    def getRoot(self, tale: dict) -> dict:
        return super().getRoot(tale)

    @access.user(TokenScope.DATA_WRITE)
    @filtermodel("folder")
    @autoDescribeRoute(
        Description(
            "Rename a run associated with a tale. Returns the renamed run folder"
        )
        .modelParam(
            "id",
            "The ID of a run",
            model=Folder,
            level=AccessType.WRITE,
            destName="rfolder",
        )
        .param(
            "name", "The new name", required=True, dataType="string", paramType="query"
        )
        .errorResponse(
            "Access was denied (if current user does not have write access to this "
            "tale)",
            403,
        )
        .errorResponse("Illegal file name", 400)
    )
    def rename(self, rfolder: dict, name: str) -> dict:
        return super().rename(rfolder, name)

    @access.public
    @filtermodel("folder")
    @autoDescribeRoute(
        Description("Returns a run.")
        .modelParam(
            "id",
            "The ID of a run.",
            model=Folder,
            level=AccessType.READ,
            destName="rfolder",
        )
        .errorResponse(
            "Access was denied (if current user does not have read access to the "
            "respective run folder.",
            403,
        )
    )
    def load(self, rfolder: dict) -> dict:
        return rfolder

    @access.user(TokenScope.DATA_WRITE)
    @filtermodel("folder")
    @autoDescribeRoute(
        Description(
            "Creates a new empty run associated with a given version and returns the new "
            "run folder. This does not actually start any computation."
        )
        .modelParam(
            "versionId",
            "A version to create the run from.",
            model=Folder,
            level=AccessType.WRITE,
            destName="version",
            paramType="query",
        )
        .param(
            "name",
            "An optional name for the run. If not specified, a name will be "
            "generated from the current date and time.",
            required=False,
            dataType="string",
            paramType="query",
        )
        .param(
            "allowRename",
            'Allow to modify "name" if object with the same name ' "already exists.",
            required=False,
            dataType="boolean",
            default=False,
        )
        .errorResponse(
            "Access was denied (if current user does not have write access to the tale "
            "associated with this version)",
            403,
        )
        .errorResponse("Illegal file name", 400)
    )
    def create(
        self, version: dict, name: str = None, allowRename: bool = False
    ) -> dict:
        user = self.getCurrentUser()
        return self.model.create(version, name, user, allowRename=allowRename)

    @access.user(TokenScope.DATA_OWN)
    @autoDescribeRoute(
        Description("Deletes a run.")
        .modelParam(
            "id",
            "The ID of run",
            model=Folder,
            level=AccessType.ADMIN,
            destName="rfolder",
        )
        .errorResponse(
            "Access was denied (if current user does not have write access to this "
            "tale)",
            403,
        )
    )
    def delete(self, rfolder: dict) -> None:
        self.model.remove(rfolder, self.getCurrentUser())

    @access.public
    @filtermodel("folder")
    @autoDescribeRoute(
        Description("Lists runs.")
        .modelParam(
            "taleId",
            "The ID of the tale to which the runs belong.",
            model=Tale,
            level=AccessType.READ,
            destName="tale",
            paramType="query",
        )
        .pagingParams(defaultSort="created")
        .errorResponse(
            "Access was denied (if current user does not have read access to this "
            "tale)",
            403,
        )
    )
    def list(self, tale: dict, limit, offset, sort):
        return super().list(
            tale, user=self.getCurrentUser(), limit=limit, offset=offset, sort=sort
        )

    @access.user(TokenScope.DATA_READ)
    @autoDescribeRoute(
        Description("Check if a run exists.")
        .modelParam(
            "taleId",
            "The ID of a tale.",
            model=Tale,
            level=AccessType.READ,
            destName="tale",
            paramType="query",
        )
        .param(
            "name",
            "Return the folder with this name or nothing if no such folder exists.",
            required=False,
            dataType="string",
        )
        .errorResponse(
            "Access was denied (if current user does not have read access to this "
            "tale)",
            403,
        )
    )
    def exists(self, tale: dict, name: str):
        return super().exists(tale, name)

    @access.user(TokenScope.DATA_READ)
    @autoDescribeRoute(
        Description(
            "Returns the status of a run in an object with two fields: status and "
            "statusString. The possible values for status, an integer, are 0, 1, 2, 3, 4, "
            "5, with statusString being, respectively, UNKNOWN, STARTING, RUNNING, "
            "COMPLETED, FAILED, CANCELLED."
        )
        .modelParam(
            "id",
            "The ID of a run.",
            model=Folder,
            level=AccessType.READ,
            destName="rfolder",
        )
        .errorResponse(
            "Access was denied (if current user does not have read access to "
            "this run)",
            403,
        )
    )
    def status(self, rfolder: dict) -> dict:
        return self.model.getStatus(rfolder)

    @access.user(TokenScope.DATA_WRITE)
    @autoDescribeRoute(
        Description(
            "Sets the status of the run. See the status query endpoint for details about "
            "the meaning of the code."
        )
        .modelParam(
            "id",
            "The ID of a run.",
            model=Folder,
            level=AccessType.WRITE,
            destName="rfolder",
        )
        .param("status", "The status code.", dataType="integer", required=True)
        .errorResponse(
            "Access was denied (if current user does not have read access to "
            "this run)",
            403,
        )
    )
    def setStatus(self, rfolder: dict, status: Union[int, RunState]) -> None:
        self.model.setStatus(rfolder, status)

    @access.user
    @autoDescribeRoute(
        Description("Start the recorded_run job")
        .modelParam(
            "id",
            "The ID of a run.",
            model=Folder,
            level=AccessType.WRITE,
            destName="run",
        )
        .param(
            "entrypoint",
            "Entrypoint command for recorded run. Defaults to run.sh",
            required=False,
            dataType="string",
            paramType="query",
        )
        .errorResponse(
            "Access was denied (if current user does not have write access to "
            "this run)",
            403,
        )
    )
    def startRun(self, run, entrypoint):
        user = self.getCurrentUser()

        if not entrypoint:
            entrypoint = "run.sh"

        runRoot = Folder().load(run["parentId"], user=user, level=AccessType.WRITE)
        tale = Tale().load(runRoot["meta"]["taleId"], user=user, level=AccessType.READ)

        resource = {
            "type": "wt_recorded_run",
            "tale_id": tale["_id"],
            "tale_title": tale["title"],
        }

        # Recorded run can run for a long time. Should we set a limit?
        token = Token().createToken(user=user, days=60)

        notification = init_progress(
            resource, user, "Recorded run", "Initializing", RECORDED_RUN_STEP_TOTAL
        )

        rrTask = recorded_run.signature(
            args=[str(run["_id"]), str(tale["_id"]), entrypoint],
            girder_job_other_fields={
                "wt_notification_id": str(notification["_id"]),
                "token": token["_id"],
            },
            girder_client_token=token["_id"],
        ).apply_async()

        Folder().setMetadata(run, {"jobId": str(rrTask.job["_id"])})

        return Job().filter(rrTask.job, user=user)

    @staticmethod
    def _expire_job_token(job):
        """Given a job's girderToken in headers set its expiration to 1h"""
        try:
            token_id = job["jobInfoSpec"]["headers"]["Girder-Token"]
        except KeyError:
            return

        if token := Token().load(token_id, force=True, objectId=False):
            token["expires"] = datetime.datetime.now(
                datetime.timezone.utc
            ) + datetime.timedelta(hours=1)
            Token().save(token)

    def updateRunStatus(self, event):
        """
        Event handler that updates the run status based on the recorded_run task.
        """
        job = event.info["job"]
        if job["title"] == "Recorded Run" and job.get("status") is not None:
            status = int(job["status"])
            rfolder = Folder().load(job["args"][0], force=True)

            # Store the previous status, if present.
            previousStatus = rfolder.get(FIELD_STATUS_CODE, -1)

            if status == JobStatus.SUCCESS:
                rfolder[FIELD_STATUS_CODE] = RunStatus.COMPLETED.code
                self._expire_job_token(job)
            elif status == JobStatus.ERROR:
                rfolder[FIELD_STATUS_CODE] = RunStatus.FAILED.code
                self._expire_job_token(job)
            elif status in (JobStatus.QUEUED, JobStatus.RUNNING):
                rfolder[FIELD_STATUS_CODE] = RunStatus.RUNNING.code

            # If the status changed, save the object
            if (
                FIELD_STATUS_CODE in rfolder
                and rfolder[FIELD_STATUS_CODE] != previousStatus
            ):
                Folder().save(rfolder)
