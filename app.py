from fastapi import FastAPI, HTTPException, Depends
from typing import List
from datetime import datetime
import os
from catboost import CatBoostClassifier
import pandas as pd
from contextlib import asynccontextmanager

from database import SessionLocal
from schema import UserGet, PostGet, FeedGet, Response
from orm_models.table_user import User
from orm_models.table_post import Post
from orm_models.table_feed import Feed
from user_split import get_group
from services.data_loading import load_features, load_posts


"""
Test hitrate catboost: 0.566; model with params optimizsation showed approximately 0.6 on test
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_models()
    load_data()
    yield


app = FastAPI(lifespan=lifespan)

users_df: pd.DataFrame = None
posts_df: pd.DataFrame = None
control_model: CatBoostClassifier = None
test_model: CatBoostClassifier = None


def get_bd():
    with SessionLocal() as session:
        return session



# Зазгразка модели
def get_model_path(exp_group: str) -> str:
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально. Немного магии
        MODEL_PATH = '/workdir/user_input/model_control' if exp_group == "control" else '/workdir/user_input/model_test'
    else:
        MODEL_PATH = 'models/new_catboost_model' if exp_group == "control" else 'models/updated_catboost_model'
    return MODEL_PATH

def load_models():
    global control_model, test_model
    control_model = CatBoostClassifier().load_model(get_model_path("control"))
    test_model = CatBoostClassifier().load_model(get_model_path("test"))
    return (control_model, test_model)


def load_data():
    global users_df, posts_df
    users_df = load_features()
    posts_df = load_posts()
    posts_df["k_words"] = posts_df["text"].apply(lambda x: len(x.replace("\n", " ").split(" ")))
    posts_df["key"] = 0


@app.get("/post/recommendations/", response_model=Response)
def recommended_post(id: int, time: datetime = None, limit: int = 10) -> Response:
    global users_df, posts_df, control_model, test_model
    
    req_user = users_df[users_df["user_id"] == id]
    if req_user.empty:
        raise HTTPException(status_code=404, detail="User was not found")
    exp_group = get_group(req_user["user_id"].values[0])
    
    if control_model is None or test_model is None:
        models = load_models()
        model = models[0] if exp_group == "control" else models[1]
    else:
        model = control_model if exp_group == "control" else test_model

    req_user["key"] = 0

    pred_df = pd.merge(posts_df, req_user, on='key').drop('key', axis=1).drop(["user_id","text"], axis=1)
    pred_df = pred_df.reindex(columns=["topic", "gender", "age", "country", "city", "exp_group", "os", "source", "k_words", "post_id"])
    pred_df["prediction"] = model.predict_proba(pred_df.drop("post_id", axis=1))[:, 1]
    rec_posts = pred_df.sort_values("prediction", ascending=False).iloc[:limit]["post_id"]

    recommended_posts = []
    for post_id in rec_posts:
        post_data = posts_df.loc[posts_df["post_id"] == post_id, ["post_id", "text", "topic"]].rename(columns={'post_id': 'id'}).iloc[0].to_dict()
        recommended_posts.append(post_data)

    return Response(exp_group=exp_group, recommendations=recommended_posts)


@app.get("/user/{id}", response_model=UserGet)
def get_user(id: int, db=Depends(get_bd)):
    result = db.query(User).filter(User.id == id).one_or_none()
    if not result:
        raise HTTPException(404, "User was not found")
    return result


@app.get("/post/{id}", response_model=PostGet)
def get_post(id: int, db=Depends(get_bd)):
    result = db.query(Post).filter(Post.id == id).one_or_none()
    if not result:
        raise HTTPException(404, "User was not found")
    return result


@app.get("/user/{id}/feed", response_model=List[FeedGet])
def get_feed_user(id: int, limit: int = 10, db=Depends(get_bd)):
    return (db.query(Feed)
              .filter(Feed.user_id == id)
              .order_by(Feed.time.desc())
              .limit(limit)
              .all())


@app.get("/post/{id}/feed", response_model=List[FeedGet])
def get_feed_post(id: int, limit: int = 10, db=Depends(get_bd)):
    return (db.query(Feed)
              .filter(Feed.post_id == id)
              .order_by(Feed.time.desc())
              .limit(limit)
              .all())
