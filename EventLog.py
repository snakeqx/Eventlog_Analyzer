import gzip
import os
import logging
import sqlite3
try:
    import xml.etree.cElementTree as ElTree
except ImportError:
    import xml.etree.ElementTree as ElTree


class EventLog:
    Database_Name = "EventLog.sqlite3.db"

    def __init__(self, filename=r"./data/a.gz"):
        if not os.path.isfile(self.Database_Name):
            self.create_database()
        self.Node = []
        self.Severity = []
        self.DateTime = []
        self.Id = []
        self.MessageId = []
        self.ComponentName = []
        self.ComponentId = []
        self.MessageText = []
        self.Type = []
        self.AssemblyName = []
        self.ProcessName = []
        self.ProcessId = []
        self.ThreadName = []
        self.AppDomain = []
        self.ClusterId = []
        try:
            self.XmlStream = gzip.GzipFile(mode="rb",
                                           fileobj=open(filename, 'rb')
                                           ).read()
            self.parse()
            self.create_database()
        except Exception as e:
            logging.error(str(e))

    def create_database(self):
        try:
            con = sqlite3.connect(self.Database_Name)
            sql_cursor = con.cursor()
            sql_string = '''create table BandAssessment(
                               uid TEXT PRIMARY KEY AUTOINCREMENT,
                               node TEXT,
                               severity TEXT,
                               datetime TEXT,
                               id TEXT,
                               message_id TEXT,
                               component_name TEXT,
                               component_id TEXT,
                               message_text TEXT,
                               type TEXT,
                               assembly_name TEXT,
                               process_id TEXT,
                               thread_name TEXT,
                               app_domain TEXT,
                               cluster_id TEXT,
                               );'''
            sql_cursor.execute(sql_string)
            logging.info(r"Database and Table created")
            con.close()
        except sqlite3.Error as e:
            logging.debug(str(e))
        finally:
            con.close()

    def parse(self):
        tree = ElTree.fromstring(self.XmlStream)
        for message in tree:
            for items in message:
                for item in items:
                    if item.tag == "Node":
                        self.Node.append(item.text)
                    if item.tag == "Severity":
                        self.Severity.append(item.text)
                    if item.tag == "DateTime":
                        self.DateTime.append(item.text)
                    if item.tag == "Id":
                        self.Id.append(item.text)
                    if item.tag == "MessageId":
                        self.MessageId.append(item.text)
                    if item.tag == "ComponentName":
                        self.ComponentName.append(item.text)
                    if item.tag == "ComponentId":
                        self.ComponentId.append(item.text)
                    if item.tag == "MessageText":
                        self.MessageText.append(item.text)
                    if item.tag == "Type":
                        self.Type.append(item.text)
                    if item.tag == "AssemblyName":
                        self.AssemblyName.append(item.text)
                    if item.tag == "ProcessName":
                        self.ProcessName.append(item.text)
                    if item.tag == "AppDomain":
                        self.AppDomain.append(item.text)
                    if item.tag == "ClusterId":
                        self.ClusterId.append(item.text)


if __name__ == '__main__':
    a = EventLog()
