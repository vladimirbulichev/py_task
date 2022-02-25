import aiohttp
from django.conf import settings
import urllib.parse
import helper.timefunctions


# Create your models here.
class AConnector:
    """Класс подключения к серверу статистики"""

    def __init__(self, host=settings.STATS['host'], login=settings.STATS['login'],
                 password=settings.STATS['password'], protocol=settings.STATS['protocol']):
        self._host = host
        self._login = login
        self._password = password
        self._protocol = protocol
        self._cookieName = 'StatServer'
        self._sessionCookie = None
        self._connection = None
        self._cookies = None
        self._cookie = None
        self._connected = False
        self._url = 'index.php'
        self._loginpage = self._protocol + self._host + self._url
        self._data = None

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, value):
        self._url = value

    @property
    def cookie(self):
        return self._cookie

    @cookie.setter
    def cookie(self, value):
        self._cookie = value

    async def authenticate(self):
        if not self._connected:
            form_data = {'username': self._login, 'password': self._password, 'option': 'login'}
            async with aiohttp.ClientSession() as session:
                async with session.post(self._loginpage, data=form_data) as resp:
                    self._data = await resp.text()
                    self._cookies = resp.cookies

            self.cookie = self._cookies[self._cookieName].value
            search_phrase = '(' + self._login + ')'
            if self._data.find(search_phrase) != -1:
                self._connected = True
        return self

    async def get_json_data(self, params={}, content_type='text/html', data={}, method='get'):
        await self.authenticate()
        cookies = {self._cookieName: self.cookie}
        async with aiohttp.ClientSession(cookies=cookies) as session:
            async with session.request(method=method, url=self._loginpage, params=params, data=data) as resp:
                text = await resp.text()
        return await resp.json(content_type=content_type)

class AStatTable:
  
    def __init__(self):
        self.columns_tuple = ()
        self._options = {}
        self._table = None
        self._rows = None

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, value):
        self._options = value

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, value):
        self._table = value

    @property
    def rows(self):
        return self._rows

    @rows.setter
    def rows(self, value):
        self._rows = value


class AStatTableJQGrid(AStatTable):
    """Таблица основанная на JQGrid"""
    def __init__(self):
        super().__init__()
        self._options = {'noheader': 1, 'action': 'jqgrid'}

    async def load_table(self):
        t = ATtConnector()
        self.table = await t.get_json_data(params=self.options, content_type='text/html')
        self.rows = None if self.table is None or self.table['rows'] is None else [d['cell'] for d in
                                                                                   self.table['rows']]
        return self


class AStatTableExtjs(AStatTable):
    """
    Таблица на ExtJs
    Данная таблица заполняется в 2 этапа. Первый POST запрос с параметрами, второй GET с получением собственно
    таблицы
    """

    def __init__(self):
        super().__init__()
        self._options = {'noheader': 1, 'a': 'extjs', 'start': 0, 'limit': 10000}
        self.connection = None

    async def connect(self):
        self.connection = ATtConnector()
        return self


class AStatTableExtjsWorkload(AStatTableExtjs):
    """
    1 url: option=mod_stat_opers&noheader=1&id=load&a=fetchStat
    2 url: option=mod_stat_opers&a=extjs&report_id=OpWorkout&noheader=1
    """
    def __init__(self, dstart, dstop, opers, show_null='no'):
        super().__init__()
        self._first_options={
            'option': 'mod_stat_opers',
            'noheader': 1,
            'id': 'load',
            'a': 'fetchStat',
        }

        self._form_data = {
            'dstart': dstart,
            'dstop': dstop,
            'show_null': show_null,
        }

        if opers is not None:
            self._form_data['opers'] = opers

    async def workload(self):
        await self.connect()
        await self.connection.get_json_data(method='POST', data=self._form_data, params=self._first_options)
        self.options['option'] = 'mod_stat_opers'
        self.options['report_id'] = 'OpWorkout'
        self.table = await self.connection.get_json_data(method='GET', params=self.options)
        self.rows = None if self.table is None or self.table['rows'] is None else [row for row in self.table['rows'] if
                                                                                   len(row['username']) > 0]
        self.rows = list(map(extend_row, self.rows))
        return self.rows
      
      
def extend_row(row):
  row['tabletime_sec'] = helper.timefunctions.intervaltosec(row['sum_worktime_wo_dinner'])
  return row
