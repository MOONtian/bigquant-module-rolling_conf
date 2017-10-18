import datetime
import logbook
import pandas as pd

from biglearning.module2.common.data import DataSource, Outputs
import biglearning.api.tools as T
import biglearning.module2.common.interface as I


# 是否自动缓存结果
bigquant_cacheable = False

# 如果模块已经不推荐使用，设置这个值为推荐使用的版本或者模块名，比如 v2
# bigquant_deprecated = '请更新到 ${MODULE_NAME} 最新版本'
bigquant_deprecated = None

# 模块是否外部可见，对外的模块应该设置为True
bigquant_public = True

# 是否开源
bigquant_opensource = True
bigquant_author = 'BigQuant'

# 模块接口定义
bigquant_category = '数据输入输出'
bigquant_friendly_name = '滚动运行配置'
bigquant_doc_url = 'https://bigquant.com/docs/'

log = logbook.Logger(bigquant_friendly_name)

def bigquant_run(
    start_date: I.str('开始日期') = '2010-01-01',
    end_date: I.str('结束日期') = '2015-12-31',
    rolling_update_days: I.int('更新周期，按自然日计算，每多少天更新一次', 1)=365,
    rolling_update_days_for_live: I.int('模拟实盘更新周期，按自然日计算，每多少天更新一次。如果需要在模拟实盘阶段使用不同的模型更新周期，可以设置这个参数', 1)=None,
    rolling_min_days: I.int('最小数据天数，按自然日计算，所以第一个滚动的结束日期是 从开始日期到开始日期+最小数据天数', 0)=365*2,
    rolling_max_days: I.int('最大数据天数，按自然日计算，0，表示没有限制，否则每一个滚动的开始日期=max(此滚动的结束日期-最大数据天数, 开始日期)', 0)=0) -> [
        I.port('滚动配置数据(DataSource pickle)', 'data'),
    ]:
    '''
    滚动运行配置。返回滚动列表，每个滚动包含开始日期和结束日期。
    '''
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    # -1 for start and end included
    rolling_min_days =  datetime.timedelta(days=rolling_min_days-1)
    rolling_max_days = datetime.timedelta(days=rolling_max_days-1) if rolling_max_days else None
    if rolling_update_days_for_live:
        if T.live_run_param('trading_date', None) is not None:
            # 如果是实盘模式
            rolling_update_days = rolling_update_days_for_live
    rolling_update_days = datetime.timedelta(days=rolling_update_days)

    rollings = []
    rolling_end_date = start_date + rolling_min_days
    while rolling_end_date <= end_date:
        if rolling_max_days is not None:
            rolling_start_date = max(rolling_end_date - rolling_max_days, start_date)
        else:
            rolling_start_date = start_date
        rollings.append({
            'start_date': rolling_start_date.strftime('%Y-%m-%d'),
            'end_date': rolling_end_date.strftime('%Y-%m-%d'),
        })
        rolling_end_date += rolling_update_days

    if not rolling_end_date:
        raise Exception('没有滚动需要执行，请检查配置')

    log.info('生成了 %s 次滚动，第一次 %s，最后一次 %s' % (
        len(rollings), rollings[0], rollings[-1]))

    # write to datasource, cached (important)
    rollings_ds = DataSource.write_pickle(rollings, use_cache=True)

    return Outputs(data=rollings_ds)


if __name__ == '__main__':
    # 测试代码
    pass
