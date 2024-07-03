from database import Base, SessionLocal
from sqlalchemy import Column, Integer, String


class Post(Base):
    __tablename__ = "post"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True)
    text = Column(String)
    topic = Column(String)
