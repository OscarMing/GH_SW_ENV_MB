from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, TIMESTAMP, text
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.schema import FetchedValue

Base = declarative_base()

class THDATA(Base):
    __tablename__ = 'thdata'
    id = Column(Integer, primary_key=True,nullable=False)
    #status = Column(String(5))
    status = Column(MEDIUMTEXT)
    info = Column(MEDIUMTEXT)
    updatetime = Column(TIMESTAMP,
                                        server_default = text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
                                        server_onupdate = FetchedValue())

class THDATA_RAW(Base):
    __tablename__ = 'thdata_raw'
    id = Column(Integer, primary_key=True,nullable=False)
    # status = Column(String(5))
    # status = Column(MEDIUMTEXT)
    info = Column(MEDIUMTEXT)
    updatetime = Column(TIMESTAMP,
                                        server_default = text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
                                        server_onupdate = FetchedValue())
class SDDATA(Base):
    __tablename__ = 'sddata'
    id = Column(Integer, primary_key=True,nullable=False)
    # status = Column(String(5))
    status = Column(MEDIUMTEXT)
    info = Column(MEDIUMTEXT)
    updatetime = Column(TIMESTAMP,
                                        server_default = text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
                                        server_onupdate = FetchedValue())

class SDDATA_RAW(Base):
    __tablename__ = 'sddata_raw'
    id = Column(Integer, primary_key=True,nullable=False)
    # status = Column(String(5))
    # status = Column(MEDIUMTEXT)
    info = Column(MEDIUMTEXT)
    updatetime = Column(TIMESTAMP,
                                        server_default = text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
                                        server_onupdate = FetchedValue())

class SDDATA_AVG(Base):
    __tablename__ = 'sddata_avg'
    id = Column(Integer, primary_key=True,nullable=False)
    info = Column(MEDIUMTEXT)
    updatetime = Column(TIMESTAMP,
                                        server_default = text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
                                        server_onupdate = FetchedValue())
    
class connection_info():
    def __init__(self, username, password, host, port, database, Base):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.engine = object()
        self.Base = Base

    def createengin(self):
        try:
            self.engine = create_engine(f'mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}', max_overflow=5,echo=False)
            return self.engine
        except Exception as e:
            return e
    
    def init_db(self):
        self.Base.metadata.create_all(self.engine)
        
    def drop_db(self):
        self.Base.metadata.drop_all(self.engine)  
    
if __name__ == '__main__':

    ci = connection_info('UserName','UserPWD','DBIP','PORT','Database Schema',Base)
    eg =ci.createengin()
    print(eg)
    ci.init_db()
    
    if not eg.dialect.has_table(eg, 'thdata'):
       ci.init_db()
    else:
       ci.drop_db()
    
