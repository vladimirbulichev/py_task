"""
асинхронная загрузка параметров
постепенно нужно переводить на нее
"""

import datetime
import asyncio
from itertools import islice
import time

from django.core.management.base import BaseCommand
import helper.datefunctions as datefunctions
from operators_positions.services import OperatorsPositionsService
from kpiloader import AKpiLoaderWorkload 


class ConsoleKpiManager:

    def __init__(self, operators_positions: OperatorsPositionsService):
        self.operators_positions_intervals = operators_positions.intervals
        self.monthdate = operators_positions.monthdate
        self._tasks = []

    def make_workload(self):
        for interval in self.operators_positions_intervals:
            dates = {'dstart': interval.sdate, 'dstop': interval.edate}
            oper_wl = AKpiLoaderWorkload(uid=interval.p_uid, month_date=self.monthdate, **dates)
            self._tasks.append(oper_wl.load())
        return self._tasks

    def get_next_part(self, count):
        n = 0
        tasks = self._tasks
        stop = count
        while True:
            r = list(islice(tasks, n, stop))
            if len(r) == 0:
                break
            yield r
            n += count
            stop += count


class Command(BaseCommand):
    help = "Asynchronous KPI params collecting"

    def add_arguments(self, parser):
        parser.add_argument('-s', '--monthdate', type=str, help='Месяц расчета')
        parser.add_argument('-p', '--param', type=int, help="Показатель")
        parser.add_argument('-a', '--account', type=str, help='Оператор')
        parser.add_argument('-m', '--prevmonth', type=int, help='Сбор данных за предыдущий месяц')

    async def run_tasks(self, tasks):
        await asyncio.gather(*tasks)

    def handle(self, *args, **options):
        stime = time.time()
        if options["prevmonth"] is not None and options["prevmonth"] == 1:
            prevmonth = datefunctions.monthdelta()
            options["monthdate"] = datetime.date(year=prevmonth.year, month=prevmonth.month, day=1).__str__()

        monthdate = options["monthdate"]
        p = options['param']
        account = options["account"]
        
        y_day = datetime.datetime.now() - datetime.timedelta(days=1)
        if monthdate is None:
            monthdate = datetime.date(year=y_day.year, month=y_day.month, day=1).__str__()
        params = {
            'monthdate': monthdate,
            'account': account
        }
        ops = OperatorsPositionsService(**params)
        kpi_loader_manager = ConsoleKpiManager(ops)

        # вырезаны условия if p == <n> or p is None:

        if p == 9 or p is None:
            """Get workload instead of p=9 at load_kpi"""
            kpi_loader_manager.make_workload()


        iter_count = 0
        simultaneous_tasks = 10
        for tasks in kpi_loader_manager.get_next_part(simultaneous_tasks):
            iter_count += 1
            asyncio.run(self.run_tasks(tasks))

        etime = time.time()
        exec_time = int(etime - stime)
        print("Расчет окончен. Время выполнения: {} с".format(exec_time))
