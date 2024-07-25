from sqlalchemy import Column, Integer, String, ARRAY, Text
from models.db_sqlalchemy import Base

class SampleCoffees(Base):
    __tablename__ = 'sample_coffees'
    __table_args__ = {'extend_existing': True}  # Add extend_existing here

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    image = Column(Text)
    ingredients = Column(ARRAY(Text))
    