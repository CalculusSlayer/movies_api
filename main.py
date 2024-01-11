from fastapi import FastAPI
import uvicorn

app = FastAPI()

# Run "curl http://localhost:8000" in terminal to test
@app.get("/") # route decorator
def read_root():
    return {"message": "Hi!!!"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)