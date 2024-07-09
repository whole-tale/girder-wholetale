#!/usr/bin/env python
# -*- coding: utf-8 -*-

from girder import events

API_VERSION = "2.1"
CATALOG_NAME = "WholeTale Catalog"
WORKSPACE_NAME = "WholeTale Workspaces"
RUNS_ROOT_DIR_NAME = "WholeTale Tale Runs"
FIELD_STATUS_CODE = "runStatus"
VERSIONS_ROOT_DIR_NAME = "WholeTale Tale Versions"
DEFAULT_IMAGE_ICON = (
    "https://raw.githubusercontent.com/whole-tale/dashboard/master/public/"
    "images/whole_tale_logo.png"
)
DEFAULT_ILLUSTRATION = (
    "https://raw.githubusercontent.com/whole-tale/dashboard/master/public/"
    "images/demo-graph2.jpg"
)


class PluginSettings:
    INSTANCE_CAP = "wholetale.instance_cap"
    DATAVERSE_URL = "wholetale.dataverse_url"
    DATAVERSE_EXTRA_HOSTS = "wholetale.dataverse_extra_hosts"
    EXTERNAL_AUTH_PROVIDERS = "wholetale.external_auth_providers"
    EXTERNAL_APIKEY_GROUPS = "wholetale.external_apikey_groups"
    ZENODO_EXTRA_HOSTS = "wholetale.zenodo_extra_hosts"
    PUBLISHER_REPOS = "wholetale.publisher_repositories"
    LOGGER_URL = "wholetale.instance_logger"
    WEBSITE_URL = "wholetale.website_url"
    DASHBOARD_LINK_TITLE = "wholetale.dashboard_link_title"
    CATALOG_LINK_TITLE = "wholetale.catalog_link_title"
    ENABLE_DATA_CATALOG = "wholetale.enable_data_catalog"
    DASHBOARD_URL = "wholetale.dashboard_url"
    LOGO = "wholetale.logo"
    DASHBOARD_TITLE = "core.brand_name"
    HEADER_COLOR = "core.banner_color"
    ABOUT_HREF = "wholetale.about_href"
    CONTACT_HREF = "wholetale.contact_href"
    BUG_HREF = "wholetale.bug_href"
    MOUNTS = "wholetale.mounts"
    HOME_DIRS_ROOT = "wholetale.homes_root"
    TALE_DIRS_ROOT = "wholetale.workspaces_root"
    VERSIONS_DIRS_ROOT = "wholetale.versions_root"
    RUNS_DIRS_ROOT = "wholetale.runs_root"
    DAV_SERVER = "wholetale.dav_server"


class SettingDefault:
    defaults = {
        PluginSettings.DASHBOARD_TITLE: "Whole Tale",
        PluginSettings.HEADER_COLOR: "#132f43",
        PluginSettings.WEBSITE_URL: "http://wholetale.org",
        PluginSettings.DASHBOARD_LINK_TITLE: "Tale Dashboard",
        PluginSettings.CATALOG_LINK_TITLE: "Data Catalog",
        PluginSettings.ENABLE_DATA_CATALOG: False,
        PluginSettings.INSTANCE_CAP: 2,
        PluginSettings.DATAVERSE_URL: (
            "https://iqss.github.io/dataverse-installations/data/data.json"
        ),
        PluginSettings.DATAVERSE_EXTRA_HOSTS: [],
        PluginSettings.EXTERNAL_AUTH_PROVIDERS: [
            {
                "name": "orcid",
                "logo": "",
                "fullName": "ORCID",
                "tags": ["publish"],
                "url": "",
                "type": "bearer",
                "state": "unauthorized",
            },
            {
                "name": "zenodo",
                "logo": "",
                "fullName": "Zenodo",
                "tags": ["data", "publish"],
                "url": "",
                "type": "apikey",
                "docs_href": "https://{siteUrl}/account/settings/applications/tokens/new/",
                "targets": [],
            },
            {
                "name": "dataverse",
                "logo": "",
                "fullName": "Dataverse",
                "tags": ["data", "publish"],
                "url": "",
                "type": "apikey",
                "docs_href": (
                    "https://{siteUrl}/dataverseuser.xhtml?selectTab=apiTokenTab"
                ),
                "targets": [],
            },
            {
                "name": "icpsr",
                "logo": "",
                "fullName": "ICPSR",
                "tags": ["data"],
                "url": "",
                "type": "apikey",
                "docs_href": "https://www.icpsr.umich.edu/mydata?path=ICPSR",
                "targets": [],
            },
        ],
        PluginSettings.EXTERNAL_APIKEY_GROUPS: [
            {"name": "zenodo", "targets": ["sandbox.zenodo.org", "zenodo.org"]},
            {
                "name": "dataverse",
                "targets": [
                    "dev2.dataverse.org",
                    "dataverse.harvard.edu",
                    "demo.dataverse.org",
                ],
            },
            {"name": "icpsr", "targets": ["www.openicpsr.org"]},
        ],
        PluginSettings.ZENODO_EXTRA_HOSTS: [],
        PluginSettings.PUBLISHER_REPOS: [
            {
                "repository": "sandbox.zenodo.org",
                "auth_provider": "zenodo",
                "name": "Zenodo Sandbox",
            },
        ],
        PluginSettings.LOGGER_URL: "http://logger:8000/",
        PluginSettings.ABOUT_HREF: "https://wholetale.org/",
        PluginSettings.CONTACT_HREF: "https://groups.google.com/forum/#!forum/wholetale",
        PluginSettings.BUG_HREF: "https://github.com/whole-tale/whole-tale/issues/new",
        PluginSettings.MOUNTS: [
            {
                "type": "data",
                "protocol": "girderfs",
                "location": "data",
            },
            {
                "type": "home",
                "protocol": "bind",
                "location": "home",
            },
            {
                "type": "workspace",
                "protocol": "bind",
                "location": "workspace",
            },
            {
                "type": "versions",
                "protocol": "girderfs",
                "location": "versions",
            },
            {
                "type": "runs",
                "protocol": "girderfs",
                "location": "runs",
            },
        ],
        PluginSettings.HOME_DIRS_ROOT: "/tmp/wt/home-dirs",
        PluginSettings.TALE_DIRS_ROOT: "/tmp/wt/tale-dirs",
        PluginSettings.RUNS_DIRS_ROOT: "/tmp/wt/runs-dirs",
        PluginSettings.VERSIONS_DIRS_ROOT: "/tmp/wt/versions-dirs",
        PluginSettings.DAV_SERVER: False,
    }


# Constants representing the setting keys for this plugin
class InstanceStatus(object):
    LAUNCHING = 0
    RUNNING = 1
    ERROR = 2
    DELETING = 3

    @staticmethod
    def isValid(status):
        event = events.trigger("instance.status.validate", info=status)

        if event.defaultPrevented and len(event.responses):
            return event.responses[-1]

        return status in (
            InstanceStatus.RUNNING,
            InstanceStatus.ERROR,
            InstanceStatus.LAUNCHING,
            InstanceStatus.DELETING,
        )


class ImageStatus(object):
    INVALID = 0
    UNAVAILABLE = 1
    BUILDING = 2
    AVAILABLE = 3

    @staticmethod
    def isValid(status):
        event = events.trigger("wholetale.image.status.validate", info=status)

        if event.defaultPrevented and len(event.responses):
            return event.responses[-1]

        return status in (
            ImageStatus.INVALID,
            ImageStatus.UNAVAILABLE,
            ImageStatus.BUILDING,
            ImageStatus.AVAILABLE,
        )


class TaleStatus(object):
    PREPARING = 0
    READY = 1
    ERROR = 2


class RunState:
    ALL = {}  # type: dict

    def __init__(self, code: int, name: str):
        self.code = code
        self.name = name
        RunState.ALL[code] = self


class RunStatus:
    UNKNOWN = RunState(0, "UNKNOWN")
    STARTING = RunState(1, "STARTING")
    RUNNING = RunState(2, "RUNNING")
    COMPLETED = RunState(3, "COMPLETED")
    FAILED = RunState(4, "FAILED")
    CANCELLED = RunState(5, "CANCELLED")

    @classmethod
    def get(cls, code: int) -> RunState:
        return RunState.ALL[code]
