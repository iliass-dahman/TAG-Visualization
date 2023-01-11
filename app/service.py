from fastapi import FastAPI
import router as router
from persistence import setup_db
from fastapi.responses import HTMLResponse
from fastapi.requests import Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import asyncio
import threading

#setting up the db
setup_db()


#create an api object, a new fastapi
app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="./static"), name="static")

#endpoint
@app.get('/', response_class=HTMLResponse)
async def Home(request: Request):
    stations = await router.get_stations()
    return templates.TemplateResponse("index.html", context={"request": request, "stations": stations})

app.include_router(router.route)

def consuming_subs():
    asyncio.create_task(router.consume_subs())

def consuming_trams():
    asyncio.create_task(router.consume_trams())

if __name__ == "__main__":
    threads = []
    threads.append(threading.Thread(target=consuming_subs))
    threads.append(threading.Thread(target=consuming_trams))
    for thread in threads:
        thread.start()
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, loop="asyncio", workers=1)
    

#@app.get("/")
#def home():
#    return {"Data": "Testing"}

#@app.get("/about")
#def about():
#    return {"Data": "About"}

#visualiser les stations les plus fréquentées
#récupère les données grâce à kafka
#données dynamiques
#retourne un dictionnaire des infos désirées