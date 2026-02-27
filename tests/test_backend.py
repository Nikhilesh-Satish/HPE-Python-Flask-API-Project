import pytest
from app import app, db

@pytest.fixture
def client():
    app.config["TESTING"]=True
    app.config["SQLALCHEMY_DATABASE_URI"]="sqlite:///:memory:"
    with app.app_context():
        db.create_all()
        yield app.test_client()
        db.drop_all()

def test_create_user(client):
    response=client.post("/api/users",json={
        "name":"Test User",
        "age":25
    })
    assert response.status_code==201
    assert response.get_json()["name"]=="Test User"

def test_get_users(client):
    client.post("/api/users",json={
        "name":"Ram",
        "age":30
    })
    response=client.get("/api/users")
    data=response.get_json()
    assert response.status_code==200
    assert len(data)==1
    assert data[0]["name"]=="Ram"
