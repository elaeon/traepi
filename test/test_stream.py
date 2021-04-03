import unittest
from src.core import Request, BuffStream, StreamPage
from src.io import Stdout
import json


class TestPageStream(unittest.TestCase):

    def test_pag(self):
        async def pprint(session, url, headers):
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    r = await response.json()
                    r = json.loads(r)
                    return Stdout(r, url)
                else:
                    return Stdout(None, url)

        buff = BuffStream(stream=StreamPage(resource="http://localhost:8080/pag", key='pageIndex'))
        request = Request(buff=buff)
        output = request.output(request.get(callback=pprint))
        tasks = request.run(output)


if __name__ == '__main__':
    unittest.main()