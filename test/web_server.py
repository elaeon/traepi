from aiohttp import web
import asyncio
import json
import datetime
import multidict
import random


def gen_data(page_index, request) -> list:
    t = []
    for i in range(random.randint(1, 10)):
        t.append({
            "id": i,
            "text": str(request.url),
            "date": datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f")})
    return t


async def handle(request):
    t = []
    for i in range(1000):
        t.append({
            "id": i,
            "text": str(request.url),
            "date": datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f")})
    text = json.dumps(t)
    await asyncio.sleep(.2)
    return web.json_response(text)


async def pagination(request):
    page_index = int(request.query.get('pageIndex', '0'))
    if page_index < 5:
        t = gen_data(page_index, request)
    else:
        t = []
    text = json.dumps(t)
    await asyncio.sleep(.2)
    return web.json_response(text)


async def pagination_header(request):
    page_index = int(request.query.get('pageIndex', '0'))
    if page_index < 5:
        t = gen_data(page_index, request)
    else:
        t = []
    text = json.dumps(t)
    await asyncio.sleep(.2)
    return web.json_response(text, headers=multidict.CIMultiDict({'next': str(int(page_index) + 1)}))


async def to_long_response(request):
    await asyncio.sleep(20)
    return web.json_response('{}')


async def random_respose(request):
    r = random.uniform(0, 1)
    t = gen_data(0, request)
    text = json.dumps(t)
    if r > .5:
        await asyncio.sleep(2)
    else:
        await asyncio.sleep(.2)
    return web.json_response(text)


app = web.Application()
app.add_routes([web.get('/1', handle),
                web.get('/2', handle),
                web.get('/3', handle),
                web.get('/pag', pagination),
                web.get('/pag_h', pagination_header),
                web.get('/long', to_long_response),
                web.get('/rand', random_respose)])


if __name__ == '__main__':
    web.run_app(app)
