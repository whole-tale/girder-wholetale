#!/usr/bin/env python
# -*- coding: utf-8 -*-


from girder.api import access
from girder.api.describe import Description, describeRoute
from girder.api.rest import Resource, filtermodel, loadmodel
from girder.constants import AccessType

from ..models.transfer import Transfer as TransferModel


class Transfer(Resource):
    def initialize(self):
        self.name = "transfer"
        self.exposeFields(
            level=AccessType.READ,
            fields={
                "_id",
                "ownerId",
                "sessionId",
                "itemId",
                "status",
                "error",
                "size",
                "transferred",
                "path",
                "startTime",
                "endTime",
            },
        )

    def validate(self, transfer):
        return transfer

    @access.user
    @filtermodel(model="transfer", plugin="wholetale")
    @describeRoute(
        Description("List transfers for a given user.")
        .param(
            "sessionId",
            "If specified, only return transfers belonging to " "a certain session.",
            paramType="path",
            required=False,
        )
        .param(
            "discardOld",
            "By default, transfers that finished more than 1 "
            "minute before this call is made are not returned. Set this to "
            '"false" to return all transfers.',
            paramType="path",
            required=False,
        )
    )
    def listTransfers(self, params):
        user = self.getCurrentUser()
        sessionId = None
        if "sessionId" in params:
            sessionId = params["sessionId"]
        discardOld = True
        if "discardOld" in params:
            discardOld = params["discardOld"] != "false"
        return list(
            TransferModel().list(user=user, sessionId=sessionId, discardOld=discardOld)
        )

    @access.user
    @loadmodel(model="session", plugin="wholetale", level=AccessType.READ)
    @filtermodel(model="transfer", plugin="wholetale")
    @describeRoute(Description("List transfers for a given user and session."))
    def listTransfersForSession(self, session, params):
        user = self.getCurrentUser()
        return list(TransferModel().list(user=user, sessionId=session["_id"]))
