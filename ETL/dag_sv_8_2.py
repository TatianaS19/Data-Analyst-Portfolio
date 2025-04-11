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

chat_id = -938659451 
my_token = '8108409746:AAGi4Lc-xjywqw4CrjWTIrTPAAJ7rbMP8iQ'
bot = telegram.Bot(token=my_token)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_sv_8_2():
    
    @task
    def extract_df():
        q = """select date, count(distinct user_id) as dau,
            sumIf(actions, action = 'message') as message_received,
            sumIf(actions, action = 'like') as likes,
            sumIf(actions, action = 'view') as viewes,
            round(likes / viewes, 3) as CTR
            from 
            (SELECT toDate(time) as date, user_id, action, count(*) as actions
            FROM simulator_20250120.feed_actions 
            where toDate(time) between (today() - 8) and yesterday()
            group by date, user_id, action
            union All
            SELECT toDate(time) as date, user_id, 'message' as action, count(*) as actions
            FROM simulator_20250120.message_actions
            where toDate(time) between (today() - 8) and yesterday()
            group by date, user_id, action)
            group by date
            order by date"""    
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task
    def extract_df2():
        q2= """SELECT toDate(time) as date, COUNT (DISTINCT post_id) as posts FROM simulator_20250120.feed_actions where toDate(time) between (today() - 8) and yesterday() GROUP BY toDate(time)"""
        df2 = ph.read_clickhouse(q2, connection=connection)
        return df2
    
    @task
    def send_metric(df, df2):
        msg1 = f'*Метрики приложения* ({str(df.date[7])[:10]})\n DAU приложения: {df.dau[7]} ({df.dau[7]/df.dau[6]-1:.2%} за день, {df.dau[7]/df.dau[0]-1:.2%} за неделю),\n Просмотры: {df.viewes[7]} ({df.viewes[7]/df.viewes[6]-1:.2%} за день, {df.viewes[7]/df.viewes[0]-1:.2%} за неделю),\n Лайки: {df.likes[7]} ({df.likes[7]/df.likes[6]-1:.2%} за день, {df.likes[7]/df.likes[0]-1:.2%} за неделю),\n CTR: {df.CTR[7]:.2f} ({df.CTR[7]/df.CTR[6]-1:.2%} за день, {df.CTR[7]/df.CTR[0]-1:.2%} за неделю),\n Направлено сообщений: {df.message_received[7]:} ({df.message_received[7]/df.message_received[6]-1:.2%} за день,{df.message_received[7]/df.message_received[0]-1:.2%} за неделю),\n Посты: {df2.posts[7]} ({df2.posts[7]/df2.posts[6]-1:.2%} за день, {df2.posts[7]/df2.posts[0]-1:.2%} за неделю)'        
        
        bot.sendMessage(chat_id=chat_id, text=msg1, parse_mode='Markdown')

            
    @task
    def send_plot(df, df2):
        plt.figure(figsize=(16, 10))
        plt.suptitle(f'Отчет по работе всего приложения\n c {(date.today() - timedelta(days=8)).strftime("%d.%m.%Y")} по {(date.today() - timedelta(days=1)).strftime("%d.%m.%Y")}', fontsize=16, y=0.98)
        
        plt.subplot(321)
        plt.plot(df.date.dt.strftime('%d %b'), df.dau, marker='o', markersize=8)
        plt.title('DAU приложения', fontsize=13)
        plt.grid(True)
        plt.tick_params(axis='both', labelsize=13)
        
        plt.subplot(322)
        plt.plot(df.date.dt.strftime('%d %b'), df.likes, marker='o', markersize=8, label = 'лайки')
        plt.plot(df.date.dt.strftime('%d %b'), df.viewes, marker='o', markersize=8, label = 'просмотры')
        plt.title('Просмотры и лайки', fontsize=13)
        plt.grid(True)
        plt.tick_params(axis='both', labelsize=13)
        plt.legend()
        
        plt.subplot(323)
        plt.plot(df.date.dt.strftime('%d %b'), df.CTR, marker='o', markersize=11)
        plt.title('CTR', fontsize=13)
        plt.grid(True)
        plt.tick_params(axis='both', labelsize=13)
        
        plt.subplot(324)
        plt.plot(df.date.dt.strftime('%d %b'), df.message_received, marker='o', markersize=11)
        plt.title('Сообщения в мессенджере', fontsize=13)
        plt.grid(True)
        plt.tick_params(axis='both', labelsize=13)
        
        plt.subplot(325)
        plt.plot(df2.date.dt.strftime('%d %b'), df2.posts, marker='o', markersize=11)
        plt.title('Посты', fontsize=13)
        plt.grid(True)
        plt.tick_params(axis='both', labelsize=13)
        
        plt.tight_layout() 
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    df = extract_df()
    df2 = extract_df2()
    send_metric(df, df2)
    send_plot(df, df2)
    
    
    
dag_sv_8_2 = dag_sv_8_2()
