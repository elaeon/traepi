import aiohttp
import asyncio
import csv
import io
import gzip
import uuid
import urllib
from abc import ABC, abstractmethod


class Pagination:
    def __init__(self, resource, key, **params):
        # {'pageIndex': 1, 'pageSize': 100}
        self.url_params = params if len(params) > 0 else \
            dict(urllib.parse.parse_qsl(urllib.parse.urlparse(resource).query))
        self.url = self.build_url(resource, params)
        self.key = key

    def __iter__(self):
        return self

    def __next__(self):
        self.url_params[self.key] = int(self.url_params.get(self.key, 0)) + 1
        return self.build_url(self.url, self.url_params)

    @staticmethod
    def build_url(url, url_params):
        return urllib.parse.urlunparse(
            urllib.parse.urlparse(url)._replace(query=urllib.parse.urlencode(url_params)))


class BuffStream:
    def __init__(self, pagination: Pagination = None, buff_size: int = 5):
        self.buff_size = buff_size
        self.stream = pagination

    def __repr__(self):
        return f'{self.__class__.__name__}'

    def __iter__(self):
        return self

    def __next__(self) -> list:
        buff = []
        for e in self.stream:
            if len(buff) < self.buff_size:
                buff.append(e)
            else:
                break
        if len(buff) > 0:
            return buff
        else:
            raise StopIteration


class Metadata(ABC):
    @staticmethod
    @abstractmethod
    def write(domain_name, content, url):
        pass


class CSVMetadata(Metadata):
    @staticmethod
    def write(domain_name, content, url):
        with open(f'{domain_name}.metadata.csv', 'a', encoding='utf-8', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([url, str(len(content))])


class Request:
    def __init__(self, buff: BuffStream):
        self.buff = buff
        self.is_buff_depleted = False

    async def get(self, headers=None, callback=None):
        async with aiohttp.ClientSession() as session:
            for batch in self.buff:
                tasks = [asyncio.create_task(callback(session, url, headers)) for url in batch]
                yield tasks
                if self.is_buff_depleted is True:
                    break

    async def output(self, coro, **kwargs):
        tasks = coro
        async for task_b in tasks:
            for task in asyncio.as_completed(task_b):
                output = await task
                if output.content is None or len(output.content) == 0:
                    self.is_buff_depleted = True
                output._write(**kwargs)

    def run(self, output):
        r = asyncio.run(output)
        return r


class Output:
    def __init__(self, content: list, url: str):
        self.content = content
        self.url = url
        self.id = str(uuid.uuid5(uuid.NAMESPACE_DNS, url))
        self.domain_name = urllib.parse.urlparse(self.url).netloc

    def _write(self, header=True, compress=None, sep='|', metadata: Metadata = CSVMetadata):
        if not self.is_valid():
            raise Exception("not csv compatible")
        names = self.get_header()
        output_io = io.StringIO()
        dict_writer = csv.DictWriter(output_io, fieldnames=names, delimiter=sep)
        if header is True:
            dict_writer.writeheader()
        for r in self.content:
            dict_writer.writerow(r)
        if compress is None:
            with open(f'{self.id}.csv', 'w', encoding='utf-8', newline='') as f:
                f.write(output_io.getvalue())
        else:
            compress(self.id, output_io)
        if metadata is not None:
            metadata.write(self.domain_name, self.content, self.url)

    def get_header(self) -> dict.keys:
        if isinstance(self.content, list):
            if isinstance(self.content[0], dict):
                return self.content[0].keys()
            else:
                fake_header = {f'col{1}': False for i in range(len(self.content))}
                return fake_header.keys()
        elif isinstance(self.content, dict):
            return self.content.keys()
        else:
            return None

    def is_valid(self) -> bool:
        return self.get_header() is not None


class Stdout:
    def __init__(self, content: list, url: str):
        self.content = content
        self.url = url
        self.id = str(uuid.uuid5(uuid.NAMESPACE_DNS, url))
        self.domain_name = urllib.parse.urlparse(self.url).netloc

    def _write(self):
        print(self.content)


def gzipf(filename, buffer):
    with gzip.GzipFile(f'{filename}.csv.gz', mode='wb') as f:
        f.write(buffer.getvalue().encode())


