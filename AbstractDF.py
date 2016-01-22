import os
import re
from datetime import date, datetime
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType, TimestampType, DateType, DoubleType, ShortType, ByteType, BooleanType

# Don Drake 
# don@drakeconsulting.com

class RDDBuilder(object):
    table_name = None
    schema_obj = None

    def __init__(self, gameDir, filename):
        #print "GAMEDIR=", gameDir
        self.gameDir = gameDir
        self.full_file = self.gameDir + '/' + filename
        self.is_postponed = False
        if not os.path.exists(self.full_file):
            #postponed
            print self.full_file
            self.is_postponed = True

        self.gameDate = self.getGameDate()
        self.gameId = self.getGameID()

        self.data = None

    def getGameDate(self):
        # Get the date from the file path
        m = re.search("gid_(\d+)_(\d+)_(\d+)_", self.full_file)
        gameDate = None
        if m:
            (year, month, day) = m.group(1,2,3)
            gameDate = date(int(year), int(month), int(day))
        #print "gameDate=", gameDate
        return gameDate

    def getGameID(self):
        m = re.search("(gid_\d+_\d+_\d+_.*)", self.gameDir)
        if m:
            #print "getGameID=", m.group(1)
            return m.group(1)
        else:
            print "getGAMEID NOT FOUND?", path
            raise Exception("Cannot find gid in path:", path)

    @staticmethod
    def getRows(gameDir):
        raise NotImplementedError("child class needs to step up...")

class AbstractDF(object):
    schema = None
    skipSelectFields = []

    def __init__(self):
        # initialize object values to be None
        for _structType in self.schema.fields:
            self.__dict__[_structType.name] = None

    def createRow(self):
        d = {}
        for _structType in sorted(self.schema.fields):
            #print "_structType=", _structType
            val = self.__dict__[_structType.name]
            #print "val=", val
            if _structType.dataType == StringType():
                val = str(val) if val is not None else None
                #print "now String"
            elif _structType.dataType == IntegerType():
                if val == '':
                    val = None
                val = int(val) if val is not None else None
                #print "now Int", val, "name=", _structType.name
            elif _structType.dataType == TimestampType():
                val = val
            elif _structType.dataType == DateType():
                if isinstance(val, str):
                    val = date.strptime(val, '%Y-%m-%d')
                elif isinstance(val, datetime):
                    val = val.date()
                elif isinstance(val, date):
                    pass
                else:
                    raise Exception("cant convert to date:" + val)
            elif _structType.dataType == DoubleType():
                val = float(val)
            elif _structType.dataType == ShortType():
                val = int(val)
            elif _structType.dataType == ByteType():
                val = int(val)
            elif _structType.dataType == BooleanType():
                val = val
            else:
                print "TYPE NOT FOUND, " + str(_structType) + "now string?"
            d[_structType.name] = val
            
        #print "CONVERTED, d=", d
        return Row(**d)

    def getSelectFields(self, df):
        cols = []
        for field in self.schema.fields:
            fieldname = field.name
            if fieldname in self.skipSelectFields:
                continue
            cols.append(df[fieldname])
        return cols

    def getSelectFieldNames(self, tableAlias):
        cols = []
        for field in self.schema.fields:
            fieldname = field.name
            if fieldname in self.skipSelectFields:
                continue
            cols.append(tableAlias + "." + fieldname)
        return ", ".join(cols)

    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

