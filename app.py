from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import logging

total_id=1


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger=logging.getLogger(__name__)


app = Flask(__name__)

app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///project.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"]
)

class Users(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(20))
    last_name = db.Column(db.String(20))
    company = db.Column(db.String(20))
    age = db.Column(db.Integer)
    city = db.Column(db.String(20))
    state = db.Column(db.String(20))
    zip_code = db.Column(db.Integer)
    email = db.Column(db.String(20))
    web = db.Column(db.String(20))


@app.route("/")
@limiter.exempt
def index():
    return "Homepage"


@app.route("/api/users", methods=["GET"])
@limiter.limit("30 per minute")
def get_users():
    logger.debug("Fetching all users from database")
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=5, type=int)
    search = request.args.get("search", default="", type=str)
    sort = request.args.get("sort", default="age", type=str)
    offset = (page - 1) * limit
    result = (
        Users.query.filter(
            (Users.first_name.ilike(f"%{search}%"))
            | Users.last_name.ilike(f"%{search}%")
        )
        .offset(offset)
        .limit(limit)
        .all()
    )
    users_list = []
    for user in result:
        users_list.append(
            {
                "id": user.id,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "company": user.company,
                "age": user.age,
                "city": user.city,
                "state": user.state,
                "zip_code": user.zip_code,
                "email": user.email,
                "web": user.web,
            }
        )
    sorted_list = []
    if sort.startswith("-"):
        sorted_list = sorted(users_list, key=lambda x: x[sort], reverse=True)
    else:
        sorted_list = sorted(users_list, key=lambda x: x[sort])
    
    logger.info(f"Fetched {len(sorted_list)} users")

    return jsonify(sorted_list)


@app.route("/api/users", methods=["POST"])
@limiter.limit("30 per minute")
def add_user():
    logger.debug(f"Adding user")
    data = request.get_json()
    new_user = Users(
        id=data["id"],
        first_name=data["first_name"],
        last_name=data["last_name"],
        company=data["company"],
        age=data["age"],
        city=data["city"],
        state=data["state"],
        zip_code=data["zip_code"],
        email=data["email"],
        web=data["web"],
    )
    db.session.add(new_user)
    db.session.commit()
    logger.debug(f"Added {new_user}")


@app.route("/api/users/<int:id>", methods=["GET"])
@limiter.limit("30 per minute")
def get_user(id):
    logger.debug(f"Getting user with id={id}")
    user = Users.query.get(id)
    if not user:
        return "User not found"

    res_dict = {
        "id": user.id,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "company": user.company,
        "age": user.age,
        "city": user.city,
        "state": user.state,
        "zip_code": user.zip_code,
        "email": user.email,
        "web": user.web,
    }

    logger.debug(f"Fetched user with id {res_dict}")
    return jsonify(res_dict)


@app.route("/api/users/<int:id>", methods=["PUT"])
def update_user(id):
    logger.debug(f"Modify some fields of id={id}")
    data = request.get_json()
    user = Users.query.filter_by(id=id).first()
    if not user:
        return jsonify({"error": "User not found"}), 404

    if "name" in data:
        user.name = data["name"]
    if "email" in data:
        user.email = data["email"]
    if "first_name" in data:
        user.first_name = data["first_name"]
    if "last_name" in data:
        user.last_name = data["last_name"]
    if "company" in data:
        user.company = data["company"]
    if "age" in data:
        user.age = data["age"]
    if "city" in data:
        user.city = data["city"]
    if "state" in data:
        user.state = data["state"]
    if "zip_code" in data:
        user.zip_code = data["zip_code"]
    if "email" in data:
        user.email = data["email"]
    if "web" in data:
        user.web = data["web"]

    db.session.commit()
    logger.debug("Modified user")


@app.route("/api/users/<int:id>", methods=["DELETE"])
def delete_user(id):
    user = Users.query.filter_by(id=id).first()
    if not user:
        return "User not found"
    db.session.delete(user)
    db.session.commit()
    total_id-=1
    logger.debug("Deleted user")


@app.route("/api/users/<int:id>", methods=["PATCH"])
def patch_user(id):
    data = request.get_json()
    user = Users.query.filter_by(id=id).first()
    for key, value in data.items():
        if hasattr(user, key):
            setattr(user, key, value)
    db.session.commit()
    logger.debug("Modified a user")


@app.route("/api/users/statistics", methods=["GET"])
def get_stats():
    total_users = db.session.query(db.func.count(Users.id)).scalar()
    avg_age = db.session.query(db.func.avg(Users.age)).scalar()
    max_age = db.session.query(db.func.max(Users.age)).scalar()
    min_age = db.session.query(db.func.min(Users.age)).scalar()
    city = (
        db.session.query(Users.city, db.func.count(Users.id)).group_by(Users.city).all()
    )

    return jsonify(
        {
            "total_users": total_users,
            "avg_age": avg_age,
            "max_age": max_age,
            "min_age": min_age,
        }
    )

@app.route("/api/users/city",methods=["GET"])
def get_user_by_partial_city():
    logger.info("Trying to retrieve city based on partial city name")
    partialCity=request.args.get("partialCity",default="",type=str)
    result = (
        Users.query.filter(
            (Users.city.ilike(f"%{partialCity}%"))
        )
        .all()
    )
    users_list = []
    for user in result:
        users_list.append(
            {
                "id": user.id,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "company": user.company,
                "age": user.age,
                "city": user.city,
                "state": user.state,
                "zip_code": user.zip_code,
                "email": user.email,
                "web": user.web,
            }
        )
    logger.info("Extracted users based on partial city")
    return jsonify(users_list)



if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        if Users.query.count() == 0:
            sample_users = [
                Users(
                    id=1,
                    first_name="John",
                    last_name="Doe",
                    company="HPE",
                    age=23,
                    city="Mysore",
                    state="Karnataka",
                    zip_code=570018,
                    email="john@abc.com",
                    web="john.com",
                ),
                Users(
                    id=2,
                    first_name="Jane",
                    last_name="Doe",
                    company="Microsoft",
                    age=25,
                    city="Bangalore",
                    state="Karnataka",
                    zip_code=570020,
                    email="jane@bc.com",
                    web="jane.com",
                ),
                Users(
                    id=3,
                    first_name="Mike",
                    last_name="Ross",
                    company="PS",
                    age=27,
                    city="Coimbatore",
                    state="Tamil Nadu",
                    zip_code=564321,
                    email="mike@abc.com",
                    web="mike.com",
                ),
                Users(
                    id=4,
                    first_name="Harvey",
                    last_name="Specter",
                    company="PS",
                    age=27,
                    city="Madurai",
                    state="Tamil Nadu",
                    zip_code=587654,
                    email="harvey@abc.com",
                    web="harvey.com",
                ),
                Users(
                    id=5,
                    first_name="Harry",
                    last_name="Potter",
                    company="Google",
                    age=45,
                    city="Hyderabad",
                    state="Telangana",
                    zip_code=587657,
                    email="harry@abc.com",
                    web="potter.com",
                ),
                Users(
                    id=6,
                    first_name="Ron",
                    last_name="Weasley",
                    company="JPMC",
                    age=44,
                    city="Mumbai",
                    state="Maharashtra",
                    zip_code=570000,
                    email="ron@abc.com",
                    web="weasley.com",
                ),
                Users(
                    id=7,
                    first_name="Severus",
                    last_name="Snape",
                    company="HPE",
                    age=60,
                    city="Mysore",
                    state="Karnataka",
                    zip_code=570018,
                    email="snape@abc.com",
                    web="snape.com",
                ),
                Users(
                    id=8,
                    first_name="Albus",
                    last_name="Dumbledore",
                    company="HP",
                    age=50,
                    city="Lucknow",
                    state="UP",
                    zip_code=570019,
                    email="albus@abc.com",
                    web="dumb.com",
                ),
                Users(
                    id=9,
                    first_name="Rubeus",
                    last_name="Hagrid",
                    company="Morgan Stanley",
                    age=70,
                    city="Ahmedabad",
                    state="Gujarat",
                    zip_code=570020,
                    email="rubeus@abc.com",
                    web="hagrid.com",
                ),
                Users(
                    id=10,
                    first_name="Draco",
                    last_name="Malfoy",
                    company="Deloitte",
                    age=39,
                    city="Jaipur",
                    state="Rajasthan",
                    zip_code=570025,
                    email="draco@abc.com",
                    web="malfoy.com",
                ),
                Users(
                    id=11,
                    first_name="Lucius",
                    last_name="Malfoy",
                    company="Infosys",
                    age=65,
                    city="Bhopal",
                    state="Madhya Pradesh",
                    zip_code=570034,
                    email="lucius@abc.com",
                    web="lucius.com",
                ),
                Users(
                    id=12,
                    first_name="Tom",
                    last_name="Riddle",
                    company="Cisco",
                    age=87,
                    city="Kolkata",
                    state="West Bengal",
                    zip_code=570234,
                    email="tom@abc.com",
                    web="riddle.com",
                ),
            ]
            total_id=12

            db.session.add_all(sample_users)
            db.session.commit()
            print("Insertion Success")
        else:
            print("Insertion Failure")
    app.run(debug=True)
