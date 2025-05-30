import urllib

from .handlers.local import Local
from .handlers.http import Http


class HandlerFactory:
    def __init__(self):
        self.loadHandlers()

    def loadHandlers(self):
        # I'm not sure if dynamic loading is worth the cost in testability, etc.
        self.handlers = {}
        self.handlers['local'] = Local
        self.handlers['http'] = Http
        self.handlers['https'] = Http
        self.handlers['file'] = Local

    def getURLTransferHandler(self, url, transferId, itemId, psPath, user, transferManager):
        if url is None or url == '':
            raise ValueError()
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme == '':
            return self.newTransferHandler('local', url, transferId, itemId, psPath, user,
                                           transferManager)
        else:
            return self.newTransferHandler(parsed.scheme, url, transferId, itemId, psPath, user,
                                           transferManager)

    def newTransferHandler(self, name, url, transferId, itemId, psPath, user, transferManager):
        if name not in self.handlers:
            raise ValueError('No such handler: "' + name + '"')
        return self.handlers[name](url, transferId, itemId, psPath, user, transferManager)
