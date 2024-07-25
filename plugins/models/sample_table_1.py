from sqlalchemy import Column, Integer, String, Sequence
from models.db_sqlalchemy import Base

class SampleTable1(Base):
    __tablename__ = 'sample_table_1'
    __table_args__ = {'extend_existing': True}  # Add extend_existing here
    
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    value = Column(Integer, default=0)
    extend_existing=True 