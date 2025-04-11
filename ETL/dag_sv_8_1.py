import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph

from datetime import datetime, date, timedelta
from airflow.decorators import dag, task

connection = {'host': 'https://clickhouse.lab.karpov.courses',
'database':'simulator_20250120',
'user':'student',
'password':'dpo_python_2020'
}

default_args = {
    'owner': 'tatjana-svinoboeva-efe5545',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 26)
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_sv_8_1():
    
    @task
    def report():
        chat_id = -938659451 
        my_token = '8108409746:AAGi4Lc-xjywqw4CrjWTIrTPAAJ7rbMP8iQ'
        bot = telegram.Bot(token=my_token)

        q1 = """
        SELECT toDate(time) as date,
               count(DISTINCT user_id) as DAU,
               countIf(user_id, action='view') as views,
               countIf(user_id, action='like') as likes,
               countIf(user_id, action='like') / countIf(user_id, action='view') as CTR
        FROM   simulator_20250120.feed_actions 
        WHERE  toDate(time) = yesterday() 
        GROUP BY date
         """
        day_rep = ph.read_clickhouse(query=q1, connection=connection)
        
        msg=f"Ключевые метрики за предыдущий день: {day_rep.date[0].date()}\n DAU:{day_rep.DAU[0]}\n Просмотры:{day_rep.views[0]}\n Лайки:{day_rep.likes[0]}\n CTR:{round(day_rep.CTR[0], 2)}"
        bot.sendMessage(chat_id=chat_id, text=msg)
        
        q2 = """
        SELECT toDate(time) as date,
               count(DISTINCT user_id) as DAU,
               countIf(user_id, action='view') as views,
               countIf(user_id, action='like') as likes,
               countIf(user_id, action='like') / countIf(user_id, action='view') as CTR
        FROM   simulator_20250120.feed_actions 
        WHERE  toDate(time) BETWEEN date_sub(DAY, 7, toDate(now())) AND date_sub(DAY, 1, toDate(now()))
        GROUP BY date
        """
        week_rep = ph.read_clickhouse(query=q2, connection=connection)
        
        fig, axes = plt.subplots(4, 1, figsize=(10, 20), sharey=False)
        fig.suptitle('Метрики за предыдущие 7 дней')
        columns_plot = ['DAU', 'views', 'likes', 'CTR']
        for i, each in enumerate(columns_plot):
            sns.lineplot(data = week_rep, ax = axes[i], x = 'date', y = each, color = 'b')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'week_rep.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    report()
    
dag_sv_8_1 = dag_sv_8_1()