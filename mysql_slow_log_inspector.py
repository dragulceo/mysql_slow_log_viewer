#!/usr/bin/python


import sqlite3
import re
import os
import sys
import time
from PyQt4.QtCore import Qt, QObject, SIGNAL, QVariant, QAbstractTableModel
from PyQt4.QtGui import QWidget, QApplication, QTableView,\
    QComboBox, QVBoxLayout, QLineEdit, QPushButton,\
    QStandardItemModel, QStandardItem, QHBoxLayout


DEFAULT_DB = 'sloq_query_log.db'
CREATE_QUERY = '''CREATE TABLE IF NOT EXISTS slow_logs \
        (timestamp integer, time text, user text, query_time real, \
        lock_time real, rows_sent integer, rows_examined integer, \
        query text, query_identifier text)'''
INSERT_QUERY_STMT = '''INSERT INTO slow_logs
        (timestamp, time, user, query_time, \
        lock_time, rows_sent, rows_examined,\
        query, query_identifier) \
        VALUES \
        (?, ?, ?, ?, ?, ?, ?, ?, ?) \
        '''
INSERT_QUERY = '''INSERT INTO slow_logs
        (timestamp, time, user, query_time, \
        lock_time, rows_sent, rows_examined,\
        query, query_identifier) \
        VALUES \
        ('%(timestamp)s', '%(time)s', '%(user)s', '%(query_time)s', \
        '%(lock_time)s', '%(rows_sent)s', '%(rows_examined)s',\
        '%(query)s', '%(query_identifier)s') \
        '''
SELECT_QUERIES = [
    'SELECT * FROM slow_logs LIMIT 1000',
    'SELECT COUNT(*) FROM slow_logs',
    'SELECT * FROM slow_logs ORDER BY query_time DESC LIMIT 100',
    'SELECT * FROM main.sqlite_master',
    'SELECT * FROM slow_logs \
        WHERE timestamp >  strftime(\'%s\',\'now\') - 2 * 24 * 3600  \
        ORDER BY query_time DESC LIMIT 200',
    'SELECT * FROM slow_logs \
            WHERE timestamp >  strftime(\'%s\',\'now\') - 15 * 24 * 3600 \
            AND query_time > 10      ORDER BY timestamp DESC LIMIT 2000'

    ]
#c.execute('''CREATE INDEX IF NOT EXISTS index_x_bound \
#        ON slow_logs (time)''')
# c.execute('''CREATE INDEX IF NOT EXISTS index_x ON plot (index_x)''')


TIME = 'TIME'
USER = 'USER'
QUERY_DATA = 'QUERY_DATA'
TIMESTAMP = 'TIMESTAMP'

STRING_LINE_STARTERS = {
    TIME: '# Time:',
    USER: '# User@',
    QUERY_DATA: '# Query_',
    TIMESTAMP: 'SET timestamp'
    }

REGEX_LINE_DATA = {
    TIME: re.compile(r'# Time:\s(?P<time>[0-9]+\s+[0-9:]+)\s.*'),
    USER: re.compile(r'(?P<user>^# User@.*)'),
    QUERY_DATA: re.compile(('# Query_time: (?P<query_time>[0-9\.]+)\s+'
                            'Lock_time: (?P<lock_time>[0-9\.]+)\s+'
                            'Rows_sent: (?P<rows_sent>[0-9]+)\s+'
                            'Rows_examined: (?P<rows_examined>[0-9]+)')),
    TIMESTAMP: re.compile(r'SET timestamp=(?P<timestamp>[0-9]+)')
    }

conn = None
cursor = None


def setCursor(c):
    global cursor
    if not c is None:
        c.execute(CREATE_QUERY)
        getConnection().commit()
    cursor = c


def getCursor():
    if cursor is not None:
        return cursor
    setCursor(getNewCursor())
    return cursor


def getNewCursor():
    return getConnection().cursor()


def getConnection():
    if conn is not None:
        return conn
    connectToDatabase(DEFAULT_DB)
    return conn


def setConnection(connection):
    global conn
    conn = connection
    conn.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
    setCursor(None)


def getNewConnection(dbName):
    return sqlite3.connect(dbName)


def connectToDatabase(dbName):
    setConnection(getNewConnection(dbName))


def getData():
    return {
        'timestamp': 0,
        'time': '',
        'user': '',
        'query_time': 0,
        'lock_time': 0,
        'rows_sent': 0,
        'rows_examined': 0,
        'query': '',
        'query_identifier': ''
        }


def getDataPieceFromLine(line):
    for key, starter in STRING_LINE_STARTERS.iteritems():
        if line.find(starter) == 0:
            result = REGEX_LINE_DATA[key].match(line)
            if result:
                return result.groupdict()
    return {}


def isTimestamp(dataPieces):
    return 'timestamp' in dataPieces


def startsNewDataPiece(dataPieces):
    return 'time' in dataPieces or 'user' in dataPieces


def insertData(data):
    #query = INSERT_QUERY % data

    data = (data['timestamp'], data['time'], data['user'],
            data['query_time'],
            data['lock_time'], data['rows_sent'], data['rows_examined'],
            data['query'], data['query_identifier'])
    return getCursor().execute(INSERT_QUERY_STMT, data)


def importToSQLite():
    if os.path.isfile(DEFAULT_DB):
        print 'Already imported'
        return

    FILENAME = 'log-slow-queries.log'
    reader = open(FILENAME, 'r')
    currentData = getData()
    i = 0
    lastTime = 0
    query = []
    for line in reader:
        query.append(line)
        dataPieces = getDataPieceFromLine(line)
        if isTimestamp(dataPieces):
            query = []
        if startsNewDataPiece(dataPieces) and currentData['query_time'] != 0:
            currentData['query'] = "".join(query[:-1])
            currentData['query_identifier'] = query[0]
            if currentData['time'] != '':
                lastTime = currentData['time']
            else:
                currentData['time'] = lastTime
            insertData(currentData)
            currentData = getData()
        for key, value in dataPieces.iteritems():
            currentData[key] = value
        i = i + 1
        if i > 10000000:
            return
    getConnection().commit()


def loadSQLiteData(query, depl=0):
    data = []
    print query
    start = time.clock()
    cur = getCursor()
    for row in cur.execute(query):
        data.append(row)
    end = time.clock()
    print start - end
    return data


class SqliteTestWindow(QWidget):
    def createComboBoxWithStrings(self, array):
        dbList = QComboBox()
        dbsListModel = QStandardItemModel(dbList)
        for f in array:
            item = QStandardItem(f)
            dbsListModel.appendRow(item)
        dbList.setModel(dbsListModel)
        return dbList

    def __init__(self, *args):
        QWidget.__init__(self, *args)

        self.tablemodel = tablemodel = SqliteTableModel(
            [], self)
        tableview = QTableView()
        tableview.setModel(tablemodel)

        dbFileMatch = re.compile(r'.*\.db$')
        files = [f for f in os.listdir('.') if os.path.isfile(f)
                 and dbFileMatch.match(f)]
        dbList = self.createComboBoxWithStrings(files)
        QObject.connect(dbList,
                        SIGNAL("currentIndexChanged(QString)"),
                        self.onDBListChanged)
        self.queryList = queryList = self.createComboBoxWithStrings(
            SELECT_QUERIES)
        self.execListPushButton = QPushButton('Exec')
        QObject.connect(self.execListPushButton,
                        SIGNAL("clicked()"),
                        self.onExecListPushButtonClicked)

        self.queryEdit = QLineEdit()
        self.execEditPushButton = QPushButton('Exec')
        QObject.connect(self.execEditPushButton,
                        SIGNAL("clicked()"),
                        self.onExecEditPushButtonClicked)

        layout = QVBoxLayout(self)
        layout.addWidget(dbList)

        hLayout = QHBoxLayout()
        hLayout.addWidget(queryList)
        hLayout.addWidget(self.execListPushButton)
        layout.addLayout(hLayout)

        hLayout = QHBoxLayout()
        hLayout.addWidget(self.queryEdit)
        hLayout.addWidget(self.execEditPushButton)
        layout.addLayout(hLayout)

        layout.addWidget(tableview)
        self.setLayout(layout)

    def onDBListChanged(self, val):
        connectToDatabase(str(val))

    def onExecListPushButtonClicked(self):
        query = str(self.queryList.currentText())
        if(len(self.queryEdit.text()) == 0):
            self.queryEdit.setText(query)
        self.tablemodel.setData(loadSQLiteData(query))

    def onExecEditPushButtonClicked(self):
        query = str(self.queryEdit.text())
        self.tablemodel.setData(loadSQLiteData(query))


class SqliteTableModel(QAbstractTableModel):
    def __init__(self, datain, parent=None, *args):
        QAbstractTableModel.__init__(self, parent, *args)
        self.arraydata = datain

    def setData(self, data):
        self.beginResetModel()
        self.arraydata = data
        self.endResetModel()

    def rowCount(self, parent):
        return len(self.arraydata)

    def columnCount(self, parent):
        if(len(self.arraydata) == 0):
            return 0
        return len(self.arraydata[0])

    def data(self, index, role):
        if not index.isValid():
            return QVariant()
        elif role != Qt.DisplayRole:
            return QVariant()
        return QVariant(self.arraydata[index.row()][index.column()])


if __name__ == "__main__":
    importToSQLite()
    app = QApplication(sys.argv)
    main_window = SqliteTestWindow()
    main_window.show()
    app.exec_()


def test():
    global DEFAULT_DB
    DEFAULT_DB = 'test.db'
