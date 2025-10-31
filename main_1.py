from fastapi import FastAPI, File, UploadFile, Depends, Form
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
import pandas as pd
import io
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.declarative import declarative_base


# Konfigurasi Database PostgreSQL
# DATABASE_URL = "postgresql://postgres:mysecret@127.0.0.1/test2" 
# DATABASE_URL = "postgresql://dhportaluser:%40B1gD4t4W4r3h0u5e@10.27.240.110:5433/md_dev"
DATABASE_URL = "postgresql://dhportaluser:%40B1gD4t4W4r3h0u5e@10.27.240.110:5433/master_data_staging" 
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# Model Tabel
class MasterElipse(Base):
    __tablename__ = "master_elipses"
    id = Column(Integer, primary_key=True, index=True)
    equip_no_unit = Column(String)
    equip_no_show = Column(String)
    equip_model_egi = Column(String)
    equip_description = Column(String)
    equip_category = Column(String)
    equip_cap_tank = Column(Float(precision=10))
    equip_fbr = Column(Float(precision=10))
    equip_position = Column(String)
    equip_owner_protes = Column(String)
    equip_owner_elipse = Column(String)
    keterangan = Column(String)

class MasterSonding(Base):
    __tablename__ = "master_sonding"
    id = Column(Integer, primary_key=True, index=True)
    station = Column(String)
    cm = Column(Float(precision=10))  
    liters = Column(Float(precision=10))  
    site = Column(String)  

class MasterStation(Base):
    __tablename__ = "master_station"
    id = Column(Integer, primary_key=True, index=True)
    fuel_station_name = Column(String)
    fuel_station_type = Column(String)
    fuel_capacity = Column(Integer)
    fuel_nozel = Column(Integer)
    site = Column(String)

class MasterUnitBanlaws(Base):
    __tablename__ = "master_unit_banlaws"
    id = Column(Integer, primary_key=True, index=True)
    unit_input = Column(String)
    unit_elipse = Column(String)
    owner = Column(String)
    pin_banlaw = Column(String)
    unit_banlaw = Column(String)

class MasterUnit(Base):
    __tablename__ = "master_unit"
    id = Column(Integer, primary_key=True, index=True)
    unit_no = Column(String)
    type = Column(String)
    brand = Column(String)
    category = Column(String)
    owner = Column(String)
    usage = Column(String)
    site = Column(String)

class OperatoFuel(Base):
    __tablename__ = "operator_fuel"
    id = Column(Integer, primary_key=True, index=True)
    JDE = Column(String)
    fullname = Column(String)
    position = Column(String)
    division = Column(String)
# Buat tabel jika belum ada
Base.metadata.create_all(bind=engine)

# Inisialisasi FastAPI
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

# Dependency untuk sesi database
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Endpoint Upload File
@app.post("/upload_elipses")
async def upload_file(
    file: UploadFile = File(...), 
    creation_by: str = Form(...),  
    db: Session = Depends(get_db)
):
    contents = await file.read()
    file_extension = file.filename.split(".")[-1]

    if file_extension not in ["xls", "xlsx"]:
        return {"status": 500,  "error": "Unsupported file format, only XLS/XLSX allowed"}

    df = pd.read_excel(io.BytesIO(contents), engine="openpyxl" if file_extension == "xlsx" else "xlrd")

    df["creation_by"] = creation_by

    try:
        df.to_sql("master_elipses", con=engine, if_exists="append", index=False)
        return { 
            "status": 200, 
            "message": "File successfully processed and data inserted into PostgreSQL"
        }

    except ValueError as e:
        return {"status": 500, "message": "Data validation error", "error": str(e)}
    except SQLAlchemyError as e:
        return {"status": 500, "message": "Database error", "error": str(e)}
    except Exception as e:
        return {"status": 500, "message": "Unknown error", "error": str(e)}

@app.post("/upload_sonding")
async def upload_file(
    file: UploadFile = File(...), 
    creation_by: str = "system",  # Tambahkan parameter creation_by
    db: Session = Depends(get_db)
):
    contents = await file.read()
    file_extension = file.filename.split(".")[-1]

    # Pastikan file formatnya adalah XLS atau XLSX
    if file_extension not in ["xls", "xlsx"]:
        return {"status": 500, "error": "Unsupported file format, only XLS/XLSX allowed"}

    # Baca data dari file Excel
    df = pd.read_excel(io.BytesIO(contents), engine="openpyxl" if file_extension == "xlsx" else "xlrd")

    # Tambahkan kolom creation_by ke DataFrame
    df["creation_by"] = creation_by

    try:
        df.to_sql("master_sonding", con=engine, if_exists="append", index=False)
        return { 
            "status": 200, 
            "message": "File successfully processed and data inserted into PostgreSQL"
        }
    except ValueError as e:
        return {"status": 500, "message": "Data validation error", "error": str(e)}
    except SQLAlchemyError as e:
        return {"status": 500, "message": "Database error", "error": str(e)}
    except Exception as e:
        return {"status": 500, "message": "Unknown error", "error": str(e)}

@app.post("/upload_station")
async def upload_file(
    file: UploadFile = File(...), 
    creation_by: str = Form(...),  
    db: Session = Depends(get_db)
):
    contents = await file.read()
    file_extension = file.filename.split(".")[-1]

    # Pastikan file formatnya adalah XLS atau XLSX
    if file_extension not in ["xls", "xlsx"]:
        return {"error": "Unsupported file format, only XLS/XLSX allowed"}

    # Baca data dari file Excel
    df = pd.read_excel(io.BytesIO(contents), engine="openpyxl" if file_extension == "xlsx" else "xlrd")

    # Tambahkan kolom creation_by ke DataFrame
    df["creation_by"] = creation_by

    try:
        df.to_sql("master_station", con=engine, if_exists="append", index=False)
        return { 
            "status": 200, 
            "message": "File successfully processed and data inserted into PostgreSQL"
        }

    except ValueError as e:
        return {"status": 500, "message": "Data validation error", "error": str(e)}
    except SQLAlchemyError as e:
        return {"status": 500, "message": "Database error", "error": str(e)}
    except Exception as e:
        return {"status": 500, "message": "Unknown error", "error": str(e)}


@app.post("/upload_unit_banlaws")
async def upload_file(
    file: UploadFile = File(...), 
    creation_by: str = Form(...),  
    db: Session = Depends(get_db)
):
    contents = await file.read()
    file_extension = file.filename.split(".")[-1]

    if file_extension not in ["xls", "xlsx"]:
        return {"status":500, "error": "Unsupported file format, only XLS/XLSX allowed"}

    df = pd.read_excel(io.BytesIO(contents), engine="openpyxl" if file_extension == "xlsx" else "xlrd")

    df["creation_by"] = creation_by

    try:
        df.to_sql("master_unit_banlaws", con=engine, if_exists="append", index=False)
        return { 
            "status": 200, 
            "message": "File successfully processed and data inserted into PostgreSQL"
        }
    except ValueError as e:
        return {"status": 500, "message": "Data validation error", "error": str(e)}
    except SQLAlchemyError as e:
        return {"status": 500, "message": "Database error", "error": str(e)}
    except Exception as e:
        return {"status": 500, "message": "Unknown error", "error": str(e)}


@app.post("/upload_unit")
async def upload_file(
    file: UploadFile = File(...), 
    creation_by: str = Form(...),  # Tambahkan parameter creation_by
    db: Session = Depends(get_db)
):
    contents = await file.read()
    file_extension = file.filename.split(".")[-1]

    # Pastikan file formatnya adalah XLS atau XLSX
    if file_extension not in ["xls", "xlsx"]:
        return {"status": 500, "error": "Unsupported file format, only XLS/XLSX allowed"}

    # Baca data dari file Excel
    df = pd.read_excel(io.BytesIO(contents), engine="openpyxl" if file_extension == "xlsx" else "xlrd")

    # Tambahkan kolom creation_by ke DataFrame
    df["creation_by"] = creation_by
    try:
        df.to_sql("master_unit", con=engine, if_exists="append", index=False)
        return { 
            "status": 200, 
            "message": "File successfully processed and data inserted into PostgreSQL"
        }
    except ValueError as e:
        return {"status": 500, "message": "Data validation error", "error": str(e)}
    except SQLAlchemyError as e:
        return {"status": 500, "message": "Database error", "error": str(e)}
    except Exception as e:
        return {"status": 500, "message": "Unknown error", "error": str(e)}

@app.post("/upload_operator")
async def upload_file(
    file: UploadFile = File(...), 
    creation_by: str = Form(...),  # Tambahkan parameter creation_by
    db: Session = Depends(get_db)
):
    contents = await file.read()
    file_extension = file.filename.split(".")[-1]

    # Pastikan file formatnya adalah XLS atau XLSX
    if file_extension not in ["xls", "xlsx"]:
        return {"status": 500, "error": "Unsupported file format, only XLS/XLSX allowed"}

    # Baca data dari file Excel
    df = pd.read_excel(io.BytesIO(contents), engine="openpyxl" if file_extension == "xlsx" else "xlrd")

    # Tambahkan kolom creation_by ke DataFrame
    df["creation_by"] = creation_by
    try:
        df.to_sql("operator_fuel", con=engine, schema="public", if_exists="append", index=False)
        return { 
            "status": 200, 
            "message": "File successfully processed and data inserted into PostgreSQL"
        }
    except ValueError as e:
        return {"status": 500, "message": "Data validation error", "error": str(e)}
    except SQLAlchemyError as e:
        return {"status": 500, "message": "Database error", "error": str(e)}
    except Exception as e:
        return {"status": 500, "message": "Unknown error", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
