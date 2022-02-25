import decimal
import calendar
from datetime import date, datetime, timedelta
from aconnector.amodels import AStatTableExtjsWorkload
from kpi.models import IndicatorValues, KpiIndicators
from helper.datefunctions import str2dateobj


class KpiLoader:
    """Сборщик KPI показателей пользователей"""
    def __init__(self, uid=None, month_date: str = None, set_sdate=None, set_edate=None, fixed=False, **kwargs):
        fmt = '%Y-%m-%d'
        if month_date is None:
            d = datetime.now() - timedelta(days=1)
            month_date = d.strftime(fmt)

        [year, month, *_] = month_date.split("-")
        dstart_obj = date(int(year), int(month), 1)
        dstop_obj = date(int(year), int(month), calendar.monthrange(int(year), int(month))[1])
        self._statdate = dstart_obj.strftime(fmt)
        self._dstart = dstart_obj.strftime(fmt) if kwargs.get('dstart') is None else kwargs.get('dstart')
        self._dstop = dstop_obj.strftime(fmt) if kwargs.get('dstop') is None else kwargs.get('dstop')
        self._fixed = fixed
        self._set_sdate = set_sdate
        self._set_edate = set_edate
        self._uid = uid

    @property
    def statdate(self):
        return self._statdate

    @statdate.setter
    def statdate(self, value):
        self._statdate = value

    @property
    def fixed(self):
        return self._fixed

    @fixed.setter
    def fixed(self, value):
        self._fixed = value

    @property
    def set_sdate(self):
        return self._set_sdate

    @set_sdate.setter
    def set_sdate(self, value):
        self._set_sdate = value

    @property
    def set_edate(self):
        return self._set_edate

    @set_edate.setter
    def set_edate(self, value):
        self._set_edate = value
        
    @property
    def uid(self):
        return self._uid

    @uid.setter
    def uid(self, value):
        self._uid= value        

    def save_element(self, row, value_field, indicator, obj, account_name='agent'):
        if row[value_field] is None:
            return False
        account = row[account_name]
        value = decimal.Decimal(row[value_field])
        has_fixed_row = False
        kpi_indicators = obj.objects.filter(account=account, indicator=indicator, statdate=self.statdate)
        if len(kpi_indicators) > 0:
            print(f'Get kpi {indicator} for account {account} at {self.statdate}')
            for kpi_indicator in kpi_indicators:
                if kpi_indicator.fixed:
                    has_fixed_row = True
                    break
                print(f'Indicator {indicator} old sdate: {kpi_indicator.sdate} new sdate: {self._dstart} old edate: {kpi_indicator.edate} new edate: {self._dstop}')
                if kpi_indicator.sdate is None or not (kpi_indicator.edate < str2dateobj(self._dstart) or kpi_indicator.sdate > str2dateobj(self._dstop)):
                    print("Delete row")
                    obj.objects.filter(id=kpi_indicator.id).delete()
        if has_fixed_row:
            return None
        print(f"Saving element row field is: {value_field} for account: {account} indicator: {indicator} value: {value}")
        co = obj.objects.create(
            account=account,
            indicator=indicator,
            statdate=self._statdate,
            sdate=self._dstart,
            edate=self._dstop,
            value=value,
            fixed=self._fixed
        )
        co.save()
        return co


class AKpiLoaderWorkload(KpiLoader):
    """Асихронный класс загрузки выработки"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.loader = AStatTableExtjsWorkload(dstart=dstart, dstop=dstop, opers=uid)

    async def load(self):
        await self.loader.workload()
        self.fill_workload()

    def fill_workload(self):
        wl_ind = KpiIndicators.objects.get(name='wl')
        pcp_wl = KpiIndicators.objects.get(name='pcp_wl')
        break_wl = KpiIndicators.objects.get(name='break_wl')
        hold_wl = KpiIndicators.objects.get(name='hold_wl')
        tt_wl = KpiIndicators.objects.get(name='tt_wl')
        work_wl = KpiIndicators.objects.get(name='work_wl')
        utz = KpiIndicators.objects.get(name='utz')
        for row in self.loader.rows:
            obj = IndicatorValues
            row["wl"] = 100 - decimal.Decimal(row["t_unawail_percent"])
            self.save_element(row, 'wl', wl_ind, obj, account_name='username')
            self.save_element(row, 't_pcp_sec', pcp_wl, obj, account_name='username')
            self.save_element(row, 't_break_sec', break_wl, obj, account_name='username')
            self.save_element(row, 't_hold_sec', hold_wl, obj, account_name='username')
            self.save_element(row, 'tabletime_sec', tt_wl, obj, account_name='username')
            self.save_element(row, 't_work_sec', work_wl, obj, account_name='username')
            self.save_element(row, 'utilization', utz, obj, account_name='username')

