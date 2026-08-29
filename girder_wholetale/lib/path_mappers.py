import pathlib
from typing import ClassVar

from ..constants import WORKSPACE_NAME


class PathMapper:
    def __init__(self):
        pass

    def girderToDav(
        self, path: pathlib.PurePosixPath | str
    ) -> pathlib.PurePosixPath:
        raise NotImplementedError()

    def girderToDavStr(self, path: pathlib.PurePosixPath | str) -> str:
        return self.girderToDav(path).as_posix()

    def davToGirder(self, path: str):
        raise NotImplementedError()

    def davToPhysical(self, path: pathlib.PurePosixPath | str) -> str:
        raise NotImplementedError()

    def girderToPhysical(self, path: pathlib.Path):
        return self.davToPhysical(self.girderToDav(path))

    def addPrefix(self, s: pathlib.PurePosixPath, n) -> pathlib.PurePosixPath:
        if s.is_absolute():
            if len(s.parts) == 1:
                raise ValueError(f"Invalid path: {s}")
            prefix = s.parts[1]
        else:
            prefix = s.parts[0]

        assert len(prefix) != 0
        if len(prefix) > n:
            prefix = prefix[0:n]

        if s.is_absolute():
            return pathlib.PurePosixPath("/", prefix, *s.parts[1:])
        else:
            return pathlib.PurePosixPath(prefix, *s.parts)

    def getSubdir(self, environ: dict):
        raise NotImplementedError()

    def girderPathMatches(self, path: pathlib.Path):
        raise NotImplementedError()

    def getRealm(self):
        raise NotImplementedError()

    def _toPosixPurePath(self, path: pathlib.Path | str):
        if isinstance(path, str):
            return pathlib.PurePosixPath(path)
        elif isinstance(path, pathlib.PurePosixPath):
            return path
        else:
            raise TypeError(f"Can't convert {path} to path")

    def isGirderRoot(self, path: pathlib.Path):
        raise NotImplementedError()


class HomePathMapper(PathMapper):
    def __init__(self):
        PathMapper.__init__(self)

    def girderToDav(
        self, path: pathlib.PurePosixPath | str
    ) -> pathlib.PurePosixPath:
        path = self._toPosixPurePath(path)
        # /user/<username>/Home/<path> -> /<username>/<path>
        return pathlib.PurePosixPath("/", path.parts[2], *path.parts[4:])

    def davToGirder(self, spath: str):
        path = pathlib.Path(spath)
        return "/user/{}/Home/{}".format(
            path.parts[1],
            "/".join(path.parts[2:]).rstrip("/"),
        )

    def davToPhysical(self, path: pathlib.PurePosixPath | str) -> str:
        path = self._toPosixPurePath(path)
        return self.addPrefix(path, 1).as_posix()

    def getSubdir(self, environ: dict) -> pathlib.PurePosixPath:
        return self.addPrefix(
            pathlib.PurePosixPath(environ["WT_DAV_AUTHORIZED_USER"]), 1
        )

    def girderPathMatches(self, path: pathlib.Path):
        return (
            len(path.parts) >= 4 and path.parts[1] == "user" and path.parts[3] == "Home"
        )

    def getRealm(self):
        return "homes"

    def isGirderRoot(self, path: pathlib.Path):
        # assume it already matches
        return len(path.parts) == 4


class TalePathMapper(PathMapper):
    def __init__(self):
        PathMapper.__init__(self)

    def girderToDav(
        self, path: pathlib.PurePosixPath | str
    ) -> pathlib.PurePosixPath:
        path = self._toPosixPurePath(path)
        # /collection/<WORKSPACE_NAME>/<WORKSPACE_NAME>/<taleId>/... -> /<taleId>/...
        return pathlib.PurePosixPath("/", *path.parts[4:])

    def davToGirder(self, spath: str):
        path = pathlib.Path(spath)
        return "/collection/{}/{}/{}".format(
            WORKSPACE_NAME,
            WORKSPACE_NAME,
            "/".join(path.parts[1:]).rstrip("/"),
        )

    def davToPhysical(self, path: pathlib.PurePosixPath | str) -> str:
        path = self._toPosixPurePath(path)
        return self.addPrefix(path, 1).as_posix()

    def getSubdir(self, environ: dict) -> pathlib.PurePosixPath:
        return self.addPrefix(pathlib.PurePosixPath(environ["WT_DAV_TALE_ID"]), 1)

    def girderPathMatches(self, path: pathlib.Path):
        # we may want to allow removal of the whole thing, and, maybe also in the case of users
        return (
            len(path.parts) >= 5
            and path.parts[1] == "collection"
            and path.parts[3] == WORKSPACE_NAME
            and path.parts[2] == WORKSPACE_NAME
        )

    def getRealm(self):
        return "tales"

    def isGirderRoot(self, path: pathlib.Path):
        return len(path.parts) == 5


class RunsPathMapper(PathMapper):
    run_to_tale: ClassVar[dict] = {}  # mutable for a reason...

    def davToPhysical(self, path: pathlib.PurePosixPath | str) -> str:
        path = self._toPosixPurePath(path)
        if path.is_absolute():
            run_id = path.parts[1]
            remainder = path.parts[2:]
        else:
            run_id = path.parts[0]
            remainder = path.parts[1:]
        path = self.run_to_tale[path.parts[1]] + "/" + run_id + "/workspace"
        if remainder:
            path += "/" + "/".join(remainder)
        path = self._toPosixPurePath(path)
        return self.addPrefix(path, 2).as_posix()

    def getSubdir(self, environ: dict) -> pathlib.PurePosixPath:
        self.run_to_tale[environ["WT_DAV_RUN_ID"]] = environ["WT_DAV_TALE_ID"]
        path_base = f"{environ['WT_DAV_TALE_ID']}/{environ['WT_DAV_RUN_ID']}/workspace"
        path = self.addPrefix(pathlib.PurePosixPath(path_base), 2)
        return path

    def getRealm(self):
        return "runs"
