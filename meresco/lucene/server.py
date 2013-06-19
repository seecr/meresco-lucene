from os.path import dirname, abspath, join
from sys import stdout

from seecr.html import DynamicHtml
from meresco.core.processtools import setSignalHandlers
from meresco.components.http import StringServer, ObservableHttpServer, BasicHttpHandler, ApacheLogger, PathFilter, PathRename, FileServer
from meresco.components.http.utils import ContentTypePlainText

from meresco.core import Observable

from weightless.io import Reactor
from weightless.core import compose, be


myPath = abspath(dirname(__file__))
dynamicPath = join(myPath, 'html', 'dynamic')
staticPath = join(myPath, 'html', 'static')

VERSION = 'dev'

def main(reactor, port):
    return \
    (Observable(),
        (ObservableHttpServer(reactor=reactor, port=port),
            (BasicHttpHandler(),
                (ApacheLogger(outputStream=stdout),
                    (PathFilter("/info", excluding=[
                            '/info/version',
                            '/info/name',
                        ]),
                        (DynamicHtml(
                                [dynamicPath],
                                reactor=reactor,
                                indexPage='/info',
                                additionalGlobals={
                                    'VERSION': VERSION,
                                }
                            ),
                        )
                    ),
                    (PathFilter("/info/version"),
                        (StringServer(VERSION, ContentTypePlainText), )
                    ),
                    (PathFilter("/info/name"),
                        (StringServer('Meresco Lucene', ContentTypePlainText),)
                    ),
                    (PathFilter("/static"),
                        (PathRename(lambda path: path[len('/static'):]),
                            (FileServer(staticPath),)
                        )
                    ),
                )
            )
        )
    )




def startServer(port):
    setSignalHandlers()
    print 'Firing up Meresco Lucene Server.'
    reactor = Reactor()

    #main
    dna = main(reactor, port)
    #/main

    server = be(dna)
    list(compose(server.once.observer_init()))

    print "Ready to rumble"
    stdout.flush()
    reactor.loop()
