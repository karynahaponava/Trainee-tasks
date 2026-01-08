import os
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASSWORD", "admin123")
DB_HOST = os.getenv("DB_HOST", "gcomm-postgres")
DB_NAME = os.getenv("DB_NAME", "gcomm_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

engine = create_engine(DATABASE_URL)

app = FastAPI(title="Storefront Sales API")

class SaleItem(BaseModel):
    purchase_id: str
    product_id: str
    total_amount: float
    quantity: int
    purchase_timestamp: Optional[datetime]

@app.get("/sales", response_model=List[SaleItem])
def get_sales(limit: int = 100):
    try:
        with engine.connect() as connection:
            query = text(f"SELECT * FROM storefront.fact_purchases LIMIT :limit")
            result = connection.execute(query, {"limit": limit})
            
            sales = []
            for row in result:
                sales.append({
                    "purchase_id": row.purchase_id,
                    "product_id": row.product_id,
                    "total_amount": float(row.total_amount) if row.total_amount else 0.0,
                    "quantity": row.quantity,
                    "purchase_timestamp": row.purchase_timestamp
                })
            return sales
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "sales-api"}