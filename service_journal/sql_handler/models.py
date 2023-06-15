from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Mapping

from sqlalchemy import create_engine, Column, Integer, String, Date, Time, Float
from sqlalchemy.orm import declarative_base, mapped_column

# TODO: Add driver and host info to config.py
engine = create_engine('mssql+pyodbc://@SQLSERVER', echo=False)

Base = declarative_base()

# models based on service_journa/sql_handler/config.py

class Actuals(Base):
    """
    The Actuals consists of a dataset of records for each individual report
     (stop or otherwise).
    """

    __tablename__ = 'v_vehicle_history'
# TODO: Add deferred property to optionally needed columns
    service_day = mapped_column(Date)
    block = mapped_column(Integer)
    trip26 = mapped_column(Integer)
    bus = mapped_column(Integer)
    Time = mapped_column(Time)
    Operator_Record_Id = mapped_column(Integer)
    Departure_Time = mapped_column(Time)
    route = mapped_column(Integer)
    dir = mapped_column(Integer)
    Stop_Id = mapped_column(Integer)
    Stop_Name = mapped_column(String)
    Boards = mapped_column(Integer)
    Alights = mapped_column(Integer)
    Onboard = mapped_column(Integer)
    OperationalStatus = mapped_column(String)
    Latitude = mapped_column(Float)
    Longitude = mapped_column(Float)

class Schedule(Base):
    """
    The Schedule consists of a dataset of records for each individual expected
    stop report.
    """

    __tablename__ = 'v_schedule_stops'

    Service_Date = mapped_column(Date)
    BlockNumber = mapped_column(Integer)
    trip26 = mapped_column(Integer)
    route_number = mapped_column(Integer)
    Direction = mapped_column(Integer)
    stop_num = mapped_column(Integer)
    departure = mapped_column(Time)

class StopLocations(Base):

    __tablename__ = 'stops'

    stop_num = mapped_column(Integer, primary_key=True)
    latitude = mapped_column(Float)
    longitude = mapped_column(Float)

class Shapes(Base):

    __tablename__ = 'shapes'

    shape_id = mapped_column(Integer, primary_key=True)
    fr_stop_num = mapped_column(Integer)
    to_stop_num = mapped_column(Integer)
    ini_date = mapped_column(Date)
    dist_ft = mapped_column(Integer)
    seg_path = mapped_column(String)

def check_exists() -> 'Mapping[bool, bool]':
    """
    Checks if the Actuals and Schedule databases exist on the server.
    """
    return (engine.dialect.has_table(engine, Schedule.__tablename__),
            engine.dialect.has_table(engine, Actuals.__tablename__))

    