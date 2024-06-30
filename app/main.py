from app.dependencies import get_db_connection
from app.sql_queries import Q1, Q2, Q3, Q4
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel, Field

app = FastAPI()

class QueryRequest(BaseModel):
    sql: str = Field(..., example="SELECT * FROM my_table WHERE id = 1;")

async def fetch_query(query):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, result)) for result in results]
    finally:
        cursor.close()
        conn.close()



@app.post("/run_query")
async def run_query(request: QueryRequest):
    # Basic validation to discourage obviously dangerous queries
    if "drop" in request.sql.lower() or "delete" in request.sql.lower():
        raise HTTPException(status_code=400, detail="Potentially dangerous operation detected.")

    results = await fetch_query(request.sql)
    return {"results": results}

@app.get("/Q1")
async def total_records():
    result = await fetch_query(Q1)
    return {"total number of records in the final table": result}

@app.get("/Q2")
async def trips_on_june_17():
    result = await fetch_query(Q2)
    return {"total number of trips started and completed on June 17th": result}

@app.get("/Q3")
async def longest_trip_day():
    result = await fetch_query(Q3)
    return {"the day of the longest trip traveled": result}

@app.get("/Q4")
async def trip_distribution():
    result = await fetch_query(Q4)
    return {"the mean, standard deviation, minimum, maximum and quartiles of the distribution": result}

@app.get("/all_answers")
async def all_answers():
    results = {
        "total number of records in the final table": await fetch_query(Q1),
        "total number of trips started and completed on June 17th": await fetch_query(Q2),
        "the day of the longest trip traveled": await fetch_query(Q3),
        "the mean, standard deviation, minimum, maximum and quartiles of the distribution": await fetch_query(Q4)
    }
    return results

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
