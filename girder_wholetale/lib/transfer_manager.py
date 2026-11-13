import logging
import os
import threading
import traceback

from girder import events
from girder.models.assetstore import Assetstore
from girder.models.file import File
from girder.models.item import Item
from girder.models.model_base import ValidationException
from girder.models.user import User
from girder.utility import assetstore_utilities

from ..constants import TransferStatus
from ..models.lock import Lock
from ..models.transfer import Transfer
from .handler_factory import HandlerFactory
from .tm_utils import TransferException, TransferHandler

logger = logging.getLogger(__name__)


class TransferThread(threading.Thread):
    def __init__(self, itemId, transferId, transferHandler, transferManager):
        threading.Thread.__init__(self, name="TransferThread[" + str(itemId) + "]")
        self.daemon = True
        self.itemId = itemId
        self.transferId = transferId
        self.transferHandler = transferHandler
        self.transferManager = transferManager

    def run(self):
        try:
            self.transferHandler.run()
            self.transferManager.transferCompleted(
                self.transferId, self.transferHandler
            )
        except Exception as ex:  # noqa
            traceback.print_exc()
            self.transferManager.transferFailed(
                self.transferId, self.transferHandler, ex
            )


class GirderDownloadTransferHandler(TransferHandler):
    def __init__(self, transferId, itemId, psPath, user, transferManager):
        TransferHandler.__init__(
            self, transferId, itemId, psPath, user, transferManager
        )

    def transfer(self):
        file = self._getFileFromItem()
        Transfer().setStatus(
            self.transferId,
            TransferStatus.TRANSFERRING,
            size=self.flen,
            transferred=0,
            setTransferStartTime=True,
        )
        stream = File().download(file, headers=False)

        try:
            os.makedirs(os.path.dirname(self.psPath))
        except OSError:
            # Right. So checking if the directory exists and then creating it is
            # prone to failure in concurrent environments. There should be a
            # function that ensures that the directory exists without failing if
            # it does. Good job, python!
            pass
        with open(self.psPath, "wb") as outf:
            self.transferBytes(outf, stream)

    def transferBytes(self, outf, stream):
        crt = 0
        for chunk in stream():
            outf.write(chunk)
            crt = crt + len(chunk)
            self.updateTransferProgress(self.flen, crt)


class TransferManager:
    def __init__(self, pathMapper):
        self.pathMapper = pathMapper
        self.handlerFactory = HandlerFactory()

    def restartInterruptedTransfers(self):
        # transfers and item.dm.transferInProgress are not atomically
        # set, so use both to figure out what needs to be re-started
        activeTransfersFromItem = Lock().listDownloadingItems()
        activeTransfers = Transfer().listAll()

        ids = set()
        data = []

        for item in activeTransfersFromItem:
            ids.add(item["_id"])
            data.append(
                {
                    "itemId": item["_id"],
                    "ownerId": item["dm"]["transfer"]["userId"],
                    "sessionId": item["dm"]["transfer"]["sessionId"],
                }
            )
        for transfer in activeTransfers:
            if transfer["itemId"] not in ids:
                data.append(transfer)

        for item in data:
            print("Restarting transfer for item " + str(item))
            try:
                user = self.getUser(item["ownerId"])
                self.startTransfer(user, item["itemId"], item["sessionId"])
            except Exception as ex:  # noqa
                logger.warning(
                    "Failed to strart transfer for itemId {}. Reason: {}".format(item["itemId"], str(ex))
                )

    def getUser(self, userId):
        return User().load(userId, force=True)

    def startTransfer(self, user, itemId, sessionId):
        pass

    def _startTransferThread(self, itemId, transferId, transferHandler):
        transferThread = TransferThread(itemId, transferId, transferHandler, self)
        transferThread.start()

    def transferCompleted(self, transferId, transferHandler):
        flen = transferHandler.getTransferredByteCount()
        Transfer().setStatus(
            transferId,
            TransferStatus.DONE,
            size=flen,
            transferred=flen,
            setTransferEndTime=True,
        )
        itemId = transferHandler.getItemId()
        psPath = transferHandler.getPhysicalPath()
        events.trigger("dm.fileDownloaded", info={"itemId": itemId, "psPath": psPath})

    def transferFailed(self, transferId, transferHandler, exception):
        if isinstance(exception, TransferException):
            temporaryFailure = not exception.isFatal()
            message = str(exception.getCause())
        else:
            temporaryFailure = False
            message = str(exception)

        if temporaryFailure:
            Transfer().setStatus(
                transferId,
                TransferStatus.FAILED_TEMPORARILY,
                error=message,
                setTransferEndTime=False,
            )
        else:
            Transfer().setStatus(
                transferId,
                TransferStatus.FAILED,
                error=message,
                setTransferEndTime=True,
            )
        itemId = transferHandler.getItemId()
        Lock().fileDownloadFailed(itemId, message)

    def transferProgress(self, transferId, total, current):
        Transfer().setStatus(
            transferId, TransferStatus.TRANSFERRING, size=total, transferred=current
        )


class SimpleTransferManager(TransferManager):
    def __init__(self, pathMapper):
        TransferManager.__init__(self, pathMapper)
        self.restartInterruptedTransfers()

    def startTransfer(self, user, itemId, sessionId):
        # add transfer to transfer DB and initiate actual transfer
        transfer = Transfer().createTransfer(user, itemId, sessionId)
        self.actualStartTransfer(user, transfer["_id"], itemId)

    def actualStartTransfer(self, user, transferId, itemId):
        Transfer().setStatus(transferId, TransferStatus.INITIALIZING)
        transferHandler = self.getTransferHandler(transferId, itemId, user)
        self._startTransferThread(itemId, transferId, transferHandler)

    def getTransferHandler(self, transferId, itemId, user):
        item = Item().load(itemId, force=True)
        psPath = self.pathMapper.getPSPath(itemId)
        files = list(Item().childFiles(item=item))
        if len(files) != 1:
            raise ValueError(
                "Wrong number of files for item "
                + str(item["_id"])
                + ": "
                + str(len(files))
            )
        file = File().load(files[0]["_id"], force=True)

        url = None
        if "linkUrl" in file:
            url = file["linkUrl"]
        elif "imported" in file:
            url = file["path"]
        elif "assetstoreId" in file:
            try:
                store = Assetstore().load(file["assetstoreId"])
                adapter = assetstore_utilities.getAssetstoreAdapter(store)
                url = adapter.fullPath(file)
            except (AttributeError, ValidationException):
                pass
        if url:
            try:
                file["size"]
            except KeyError:
                raise ValueError(
                    "File {} must have a size attribute.".format(str(file["_id"]))
                )
            return self.handlerFactory.getURLTransferHandler(
                url, transferId, itemId, psPath, user, self
            )
        else:
            return GirderDownloadTransferHandler(transferId, itemId, psPath, user, self)
