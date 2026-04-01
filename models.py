from sqlalchemy import Column, String, Integer, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base
import pandas as pd


class BaseMixin:
    @classmethod
    def export_to_csv(cls, session, filename=None):
        query = session.query(cls)
        # 以selection_run_id 排序
        if hasattr(cls, 'selection_run_id'):
            query = query.order_by(cls.selection_run_id)
        # elif hasattr(cls, 'cmd_id'):
        #     query = query.order_by(cls.cmd_id)
        # elif hasattr(cls, 'order_no'):
        #     query = query.order_by(cls.order_no)
            
        results = query.all()
        data = [
            {c.name: getattr(row, c.name) for c in cls.__table__.columns}
            for row in results
        ]
        
        df = pd.DataFrame(data)
        file_name = filename or f"{cls.__tablename__}.csv"
        df.to_csv(f"DB/{file_name}", index=False, encoding="utf-8-sig")
        print(f"export to csv success：{file_name}")


Base = declarative_base(cls=BaseMixin)

class CfgLocation(Base):
    __tablename__ = 'cfg_location'
    __table_args__ = {'schema': 'dev'}
    
    location_id = Column(String, primary_key=True)
    row_x = Column(String)
    bay_y = Column(String)
    level_z = Column(String)

class CurCarrier(Base):
    __tablename__ = 'cur_carrier'
    __table_args__ = {'schema': 'dev'}
    
    carrier_id = Column(String, primary_key=True) 
    parent_carrier_id = Column(String)
    location_id = Column(String)
    scenario = Column(String, primary_key=True)

class CurInventory(Base):
    __tablename__ = 'cur_inventory'
    __table_args__ = {'schema': 'dev'}
    
    location_id = Column(String, primary_key=True)
    carrier_id = Column(String, primary_key=True)
    material_id = Column(String)
    material_quantity = Column(Integer)
    scenario = Column(String, primary_key=True)

class CurCmdMaster(Base):
    __tablename__ = 'cur_cmd_master'
    __table_args__ = {'schema': 'dev'}
    
    cmd_id = Column(Integer, primary_key=True)
    cmd_type = Column(String)
    source_position = Column(String)
    dest_position = Column(String)
    parent_carrier_id = Column(String)
    create_user = Column(String)
    create_time = Column(DateTime)
    batch_run_id = Column(String)
    order_scenario = Column(String)
    inv_scenario = Column(String)
    selection_algo_ver = Column(String)
    selection_run_id = Column(String, primary_key=True) 
    batch_time_window = Column(Integer)
    batch_algo_ver = Column(String)

class CurCmdDetail(Base):
    __tablename__ = 'cur_cmd_detail'
    __table_args__ = {'schema': 'dev'}
    
    cmd_id = Column(Integer, primary_key=True)
    order_line_id = Column(String, primary_key=True)
    carrier_id = Column(String,  primary_key=True)
    quantity = Column(Integer)
    create_user = Column(String)
    create_time = Column(DateTime)
    update_user = Column(String)
    update_time = Column(DateTime)

# class CurOrderMaster(Base):
#     __tablename__ = 'cur_order_master'
#     __table_args__ = {'schema': 'dev'}
    
#     order_no = Column(String, primary_key=True)
#     order_date = Column(DateTime)
#     put_wall_group = Column(String)
#     create_user = Column(String)
#     create_time = Column(DateTime)
#     update_user = Column(String)
#     update_time = Column(DateTime)
#     original_order_no = Column(String)
#     scenario = Column(String, primary_key=True)

# class CurOrderDetail(Base):
#     __tablename__ = 'cur_order_detail'
#     __table_args__ = {'schema': 'dev'}
    
#     order_line_id = Column(String, primary_key=True)
#     order_no = Column(String, primary_key=True)
#     material_id = Column(String)
#     quantity = Column(Integer)
#     dest_position = Column(String)
#     dest_storage_id = Column(String)
#     update_user = Column(String)
#     update_time = Column(DateTime)
#     scenario = Column(String, primary_key=True)