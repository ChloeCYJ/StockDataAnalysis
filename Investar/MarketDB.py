import pymysql
import pandas as pd
import calendar
import re

class MarketDB:
    def __init__(self):
        """생성자 : MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn=pymysql.connect(host='localhost',user='root',password='rootadmin',port=3300, db='investar', charset='utf-8')
        self.codes={}
        self.get_comp_info()

    def __del__(self):
        """소멸자 : MariaDB 연결해제"""
        self.conn.close()

    def get_comp_info(self):
        """company_info 테이블에서 읽어와서 codes에 저장"""
    

    def get_daily_price(self, code, start_date=None, end_date=None):
        """KRX 종목별 시세를 데이터 프레임 형태로 반환"""
        if start_date is None:
            one_year_ago=datetime.today()-timedelta(days=365)
            start_date=one_year_ago.strftime('%Y-%m-%d')
            print("start_date is initialized to '{}'".format(start_date))
        sql=f"select * from daily_price where code='{code}'"\
            f"and date>='{start_date}'and date<='{end_date}'"
            df=pd.read_sql(sql,self.conn)
            df.index=df['date']
            return df
