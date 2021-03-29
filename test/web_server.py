from aiohttp import web
import asyncio
import json
import datetime


async def handle(request):
    t = []
    for i in range(1000):
        t.append({"text": str(request.url),
                  "date": datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f")})
    text = json.dumps(t)
    await asyncio.sleep(.2)
    return web.json_response(text)


async def pagination(request):
    page_index = int(request.query.get('pageIndex', '0'))
    t = []
    if page_index < 5:
        t.append({"text": str(request.url),
                  "date": datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f")})

    text = json.dumps(t)
    await asyncio.sleep(.2)
    return web.json_response(text)


app = web.Application()
app.add_routes([web.get('/1', handle),
                web.get('/2', handle),
                web.get('/3', handle),
               web.get('/pag', pagination)])


if __name__ == '__main__':
    web.run_app(app)