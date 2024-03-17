import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import date
import io
import sys
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses', 
'database':'simulator',
'user':'student',
'password':'dpo_python_2020'
}

schedule_interval = '*/15 * * * *' # проверяем каждые 15 минут

default_args = {
    'owner': 's-c',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 2, 17)
}

#функция на выгрузку данных
def make_data():
    q = ''' SELECT
                          toStartOfFifteenMinutes(time) timestamp,
                         toDate(timestamp) date,
                         formatDateTime(timestamp, '%R') as hm,
                         uniqExact(user_id) as active_users_feed,
                         countIf(action = 'view') views,
                         countIf(action = 'like') likes,
                         likes/views CTR
                    FROM simulator_20240120.feed_actions
                    WHERE timestamp >=  today() - 1 and timestamp < toStartOfFifteenMinutes(now())
                    GROUP BY timestamp, date, hm
                    ORDER BY timestamp '''

    data = ph.read_clickhouse(q, connection=connection)
    return data

#функция на подсчет квантилей
def make_quantiles(data):
    
    metrics = ['active_users_feed','views', 'likes', 'CTR']
    
    for metric in metrics:
        data[f'{metric}_q25'] = data[f'{metric}'].shift(1).rolling(4).quantile(0.25)
        data[f'{metric}_q75'] = data[f'{metric}'].shift(1).rolling(4).quantile(0.75)
        data[f'{metric}_IQR'] = data[f'{metric}_q75'] - data[f'{metric}_q25']
        data[f'{metric}_lower'] = data[f'{metric}_q25'] - 1.5 * data[f'{metric}_IQR']
        data[f'{metric}_upper'] = data[f'{metric}_q75'] + 1.5 * data[f'{metric}_IQR']
        
    return data
    
#Детектор аномалий. 
def check_anomaly(data, metric):
    
    if data[metric].iloc[-1] < data[f'{metric}_lower'].iloc[-1] or data[metric].iloc[-1] > data[f'{metric}_upper'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, data

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, concurrency=3)

def anomaly_report():
     
    @task()
    def run_feed():
        chat_id = 303597246
        bot = telegram.Bot(token='***')
        
        metrics_list = ['active_users_feed', 'views', 'likes', 'CTR']
        
        data = make_data()
        data = make_quantiles(data)
        
        
        for metric in metrics_list:
            is_alert, data = check_anomaly(data, metric)
            if is_alert == 1:
                
                msg = f'''Метрика {metric}:\nтекущее значение = {data[metric].iloc[-1]}\nне входит в доверительный интервал\nСсылка на дашборд:\nhttp://superset.lab.karpov.courses/r/4905'''
             
                #параметры графиков
                sns.set(rc={'figure.figsize': (15, 10)})
                plt.tight_layout()

                ax = sns.lineplot(x=data['timestamp'], y=data['users_lenta'], label=f'{metric}')
                ax = sns.lineplot(x=data['timestamp'], y=data['upper'], label='upper')
                ax = sns.lineplot(x=data['timestamp'], y=data['lower'], label='lower')
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()

               
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        return
    
    run_feed = run_feed()
    
anomaly_report = anomaly_report()
