__author__ = 'sarink'

import xml.sax


class XmlHandler(xml.sax.ContentHandler):

    def __init__(self, root_node):
        self.nodes = [('root', root_node)]
        self.current_text = ''

    def startElement(self, name, attrs):
        self.current_text = ''
        new_node = self.nodes[-1][1].startElement(name, attrs)
        if new_node is not None:
            self.nodes.append((name, new_node))

    def endElement(self, name):
        if self.nodes[-1][0] == name:
            if hasattr(self.nodes[-1][1], 'endNode'):
                self.nodes[-1][1].endNode()
            self.nodes.pop()
        else:
            self.nodes[-1][1].endElement(name, self.current_text)
        self.current_text = ''

    def characters(self, content):
        self.current_text += content