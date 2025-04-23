from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, Boolean, DateTime, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import datetime
import jwt
from starlette.middleware.cors import CORSMiddleware



from routers import call_cdr_in  # ✅ Fixed incorrect router name (was `call_summary`)

# Secret key for JWT
SECRET_KEY = "your-secret-key"

# Database setup
DATABASE_URL = "mysql+pymysql://root:dial%40mas123@172.12.10.22/db_dialdesk?charset=utf8mb4"
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

DATABASE_URL1 = "mysql+pymysql://root:dial%40mas123@192.168.10.12/db_dialdesk?charset=utf8mb4"
engine1 = create_engine(DATABASE_URL1, echo=True)
SessionLocal1 = sessionmaker(autocommit=False, autoflush=False, bind=engine1)

Base = declarative_base()


# DB dependency for main DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# DB dependency for secondary DB
def get_db1():
    db = SessionLocal1()
    try:
        yield db
    finally:
        db.close()


# Model for User
class TblUser(Base):
    __tablename__ = 'tbl_user'
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, index=True)
    password = Column(String)
    password2 = Column(String)
    name = Column(String, nullable=True)
    email = Column(String, nullable=True)
    phone = Column(String, nullable=True)
    designation = Column(String, nullable=True)
    user_type = Column(String, nullable=True)
    user_right = Column(String, nullable=True)
    user_active = Column(Boolean, default=True)
    update_user = Column(String, nullable=True)
    update_date = Column(DateTime, default=datetime.datetime.utcnow)
    access = Column(String, nullable=True)
    parent_access = Column(String, nullable=True)
    create_id = Column(Integer, nullable=True)
    create_at = Column(DateTime, default=datetime.datetime.utcnow)
    token = Column(String, nullable=True)


Base.metadata.create_all(bind=engine)

# FastAPI app setup
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register router
app.include_router(call_cdr_in.router, prefix="/api")  # ✅ Corrected router name

# Pydantic models
class UserCreate(BaseModel):
    username: str
    password: str
    password2: str
    name: str = None
    email: str = None
    phone: str = None
    designation: str = None
    user_type: str = None
    user_right: str = None
    user_active: bool = True
    access: str = None
    parent_access: str = None
    create_id: int = None


class LoginRequest(BaseModel):
    email: str
    password: str


# User creation
@app.post("/create_user/")
def create_user(user: UserCreate):
    if user.password != user.password2:
        raise HTTPException(status_code=400, detail="Passwords do not match")

    db = SessionLocal()
    db_user = TblUser(
        username=user.username,
        password=user.password,
        password2=user.password2,
        name=user.name,
        email=user.email,
        phone=user.phone,
        designation=user.designation,
        user_type=user.user_type,
        user_right=user.user_right,
        user_active=user.user_active,
        access=user.access,
        parent_access=user.parent_access,
        create_id=user.create_id,
        create_at=datetime.datetime.utcnow()
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    db.close()
    return {"message": "User created successfully", "user_id": db_user.id}


# Login
@app.post("/login/")
def login(user: LoginRequest):
    db = SessionLocal()
    db_user = db.query(TblUser).filter(TblUser.email == user.email).first()

    if not db_user:
        db.close()
        raise HTTPException(status_code=404, detail="User not found")

    if db_user.password != user.password:
        db.close()
        raise HTTPException(status_code=401, detail="Incorrect password")

    # Generate JWT Token
    token = jwt.encode(
        {"email": user.email, "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=2)},
        SECRET_KEY,
        algorithm="HS256"
    )

    # Update token in DB
    db_user.token = token
    db.commit()
    db.refresh(db_user)
    db.close()

    return {
        "message": "Login successful",
        "user_id": db_user.id,
        "email": db_user.email,
        "token": db_user.token,
        "username": db_user.username
    }


# Utility function to decode JWT token and verify
def decode_jwt(authorization: str):
    try:
        payload = jwt.decode(authorization, SECRET_KEY, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


# Auth-protected route using JWT token
@app.get("/active_companies")
def get_active_companies(
        authorization: str = Header(None),  # Token received via Header
        db: Session = Depends(get_db1),
        user_db: Session = Depends(get_db)
):
    print("Authorization token:", authorization)  # This should print the token if passed correctly

    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization token is missing")

    # Extract the token (in case of 'Bearer <token>')
    token = authorization.split("Bearer ")[-1]

    if not token:
        raise HTTPException(status_code=401, detail="Authorization token is invalid")

    # Verify the token
    payload = decode_jwt(token)

    # Extract the email from the decoded JWT payload
    email = payload.get("email")
    if not email:
        raise HTTPException(status_code=401, detail="Invalid token: Email not found")

    db_user = user_db.query(TblUser).filter(TblUser.email == email).first()
    if not db_user or db_user.token != token:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    # Query to fetch active companies from the database
    query = text(""" 
        SELECT company_id, 
               CONCAT(UCASE(LEFT(company_name, 1)), 
                      LCASE(SUBSTRING(company_name, 2))) AS label 
        FROM registration_master 
        WHERE status = 'A' 
        ORDER BY label
    """)
    result = db.execute(query).fetchall()
    return [dict(row._mapping) for row in result]







