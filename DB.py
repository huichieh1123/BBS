from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import (
    Base, CurCarrier, CfgLocation, CurInventory, 
    CurCmdMaster, CurCmdDetail
)
from sqlalchemy.pool import NullPool
import pandas as pd

from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Construct the SQLAlchemy connection string
DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL, poolclass=NullPool)

# Test the connection
try:
    with engine.connect() as connection:
        print("Connection successful!")

    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Export all models defined in models.py
        CurCarrier.export_to_csv(session)
        CfgLocation.export_to_csv(session)
        CurInventory.export_to_csv(session)
        CurCmdMaster.export_to_csv(session)
        CurCmdDetail.export_to_csv(session)
        CurOrderMaster.export_to_csv(session)
        CurOrderDetail.export_to_csv(session)
        
    except Exception as e:
        print(f"Operation failed: {e}")
    finally:
        session.close()

except Exception as e:
    print(f"Failed to connect: {e}")
