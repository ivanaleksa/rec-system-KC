from database import Base
from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey
from sqlalchemy.orm import relationship
from table_post import Post
from table_user import User


class Feed(Base):
    __tablename__ = "feed_action"
    __table_args__ = {"schema": "public"}

    user_id = Column(Integer, ForeignKey(User.id), primary_key=True, name="user_id")
    user = relationship("User")
    post_id = Column(Integer, ForeignKey(Post.id), primary_key=True, name="post_id")
    post = relationship("Post")
    action = Column(String, name="action")
    time = Column(TIMESTAMP)
