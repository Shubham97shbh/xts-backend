from fastapi import FastAPI
import pandas as pd
from pydantic import BaseModel

class Data(BaseModel):
    id: int
    ltp: float

app = FastAPI()
df = pd.DataFrame(columns=["id", "ltp"])

@app.put("/update_data")
def update_data(data: Data):
    global df
    id, ltp = data.id, data.ltp
    if id in df["id"].values:
        df.loc[df["id"] == id, "ltp"] = ltp
    else:
        new_row = pd.DataFrame({"id": [id], "ltp": [ltp]})
        df = pd.concat([df, new_row], ignore_index=True)
    return {"status_code": 200}


@app.get("/get_data")
def get_data(data: dict):
    print("DATA:", data)
    print(df)
    row_index = df[df["id"] == data["id"]].index
    if len(row_index) == 0:
        return {"message": "No data found", "ltp": 0}
    return {"message": df.loc[row_index, "ltp"]}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="localhost", port=8001)
