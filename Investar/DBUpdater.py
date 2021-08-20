import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import json,calendar, time, pymysql
from threading import Timer


class DBUpdater:
    def __init__(self):
        """생성자:Maria DB 연결 및 종목코드 딕셔너리 생성"""
        self.conn=pymysql.connect(host='localhost',port=3300, user='root',passwd='rootadmin',db='INVESTAR',charset='utf8')

        with self.conn.cursor() as curs:
            sql="""
            CREATE TABLE IF NOT EXISTS company_info(
                CODE VARCHAR(20),
                company VARCHAR(40),
                last_update DATE,
                PRIMARY KEY (CODE)
            )"""
	
            curs.execute(sql)
            sql="""
            CREATE TABLE IF NOT EXISTS daily_price(
                CODE VARCHAR(40),
                DATE DATE,
                oepn Bigint(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                diff BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY  (CODE,DATE)
            );
            """
            curs.execute(sql)
        self.conn.commit()
        self.codes=dict()
   

    def __del__(self):
        """소멸자:MariaDB 연결해제"""
        self.conn.close()

    def read_krx_code(self):
        """KRX로부터 상장법인목록 파일을 읽어와서데이터프레임으로 반환"""
        
        url='http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        krx=pd.DataFrame()
        krx=krx.append(pd.read_html(requests.get(url,headers={'User-agent':'Mozilla/5.0'}).text)[0])
        krx=krx[['종목코드','회사명']]

        krx=krx.rename(columns={'종목코드':'code','회사명':'company'})
        krx.code=krx.code.map('{:06d}'.format)

        return krx


    def update_comp_info(self): #여기 다시한번 확인 필요.
        """종목코드를 company_info 테이블에 업데이트한 후 딕셔너리에 저장"""
        sql="select * from company_info"
        df=pd.read_sql(sql,self.conn) #df= 000020 동화약품 2021-07-28
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]]=df['company'].values[idx] #codes[코드명]=회사명
            """
            df['code'] = idx code 값 //시리즈 format
            df['code'].values[0] = 000020
            df['company'].values[0] =동화약품
            codes[df['code].values[0]]=df['company'].values[0]  //즉 codes[000020]='동화약품'. codes[코드명]=회사명임. (딕셔너리 형)

            codes[df['code'].values[0]]=동화약품 //codes[코드명]=회사명
            codes['000020'] = 회사명 //직접 코드명을 쳤을 때 회사명이 출력되는 것을 볼 수 있음.
            """
        with self.conn.cursor() as curs:
            sql="select max(last_update) from company_info"
            curs.execute(sql)
            rs=curs.fetchone() #max date
            today=datetime.today().strftime('%Y-%m-%d')

            if rs[0]==None or rs[0].strftime('%Y-%m-%d')<today: #데이터가 없거나, 과거의 데이터만 존재하는 경우
                krx=self.read_krx_code() #현재 pandas로 받은 krx데이터를 변수에 리스트로 반환.
               
                for idx in range(len(krx)):
                    code=krx.code.values[idx]
                    company=krx.company.values[idx]
                    sql=f"REPLACE INTO company_info (code, company, last_update)"\
                        f"values ('{code}','{company}','{today}')"
                        #Key 값을 기준으로 데이터를 replace함.
                    curs.execute(sql)
                    self.codes[code]=company

                    tmnow=datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] {idx:04d} REPLACE INTO company_info"\
                        f"VALUES({code},{company},{today})") #log 용
                    self.conn.commit()
                    print('')

    def read_naver(self,code,company,pages_to_fetch):
        """네이버 금융에서 주식시세를 읽어서 데이터프레임으로 반환"""
        try:
            
            url=f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            html=BeautifulSoup(requests.get(url, headers={'User-agent':'Mozilla/5.0'}).text,"lxml")
            pgrr=html.find("td",class_="pgRR")
            if pgrr is None:
                return None
            
            s=str(pgrr.a["href"]).split('=')
            lastpage=s[-1]
            df=pd.DataFrame()
           
            pages=min(int(lastpage),pages_to_fetch) #parameter- pages_to_fetch vs 최종 페이지 중 작은 값을 세팅
            for page in range(1,pages+1): #i~pages까지 for문 실행.
                pg_url='{}&page={}'.format(url,page)
                df=df.append(pd.read_html(requests.get(pg_url,headers={'User-agent':'Mozilla/5.0'}).text)[0])
                tmnow=datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d} / {:04d} pages are downloading... OK?'.format(tmnow, company, code, page, pages),end="")
              
                df=df.rename(columns={'날짜':'date','종가':'close','전일비':'diff','시가':'open','고가':'high',
                '저가':'low','거래량':'volume'})
                df['date']=df['date'].replace('.','-')
                df=df.dropna()

                df[['close','diff','open','high','low','volume']]=df[['close','diff','open','high','low','volume']].astype(int)
                #Int Type으로 변경. 247979.0 -> 247979
                df=df[['date','open','high','low','close','diff','volume']]
                

        except Exception as e:
            print('Exception occured :',str(e))
            return None

        return df
            
    def replace_into_db(self,df,num,code,company):
        """네이버금융에서 읽어온 주식 시세를 DB에 Replace"""
        
        with self.conn.cursor() as curs:
            for r in df.itertuples(): #데이터 프레임을 tuple로 변경처리.
                sql ="REPLACE INTO daily_price values('{}','{}',{},{}, "\
                    "{},{},{},{})".format(code,r.date,r.open,r.high,r.low,r.close,r.diff,r.volume)
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) : {} rows>REPLACE INTO daily_price'\
                ' [OK]'.format(datetime.now().strftime('%Y-%m-%d'\
                ' %H:%M'), num+1, company, code,len(df)))


    def update_daily_price(self, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""
        for idx, code in enumerate(self.codes): 
            #enumerate는 순서가 있는 자료형(리스트, 튜플, 문자열)을 입력으로 받아 인덱스 값을 포함하는 enumerate 객체를 리턴한다
            df=self.read_naver(code,self.codes[code],pages_to_fetch) 
            # Log check ===> print("code='{}',self.codes[code]='{}',pages_to_fetch={}".format(code,self.codes[code],pages_to_fetch))
            # code, 회사명, pages_to_fetch. 
            # 즉 krx에 등록된 모든 코드(회사명)에 대한 주식에 대해 네이버 조회해서 데이터 프레임화시킴.
            
            if df is None:
                print('df is none?')
                continue

            self.replace_into_db(df,idx,code,self.codes[code]) #데이터 DB에 업데이트

    def execute_daily(self):
        """실행 즉시 및 매일 오후 다섯시에 daily_price테이블 업데이트"""
        self.update_comp_info()
        try:
            with open('config.json','r') as in_file:
                config=json.load(in_file)
                pages_to_fetch=config['pages_to_fetch']
                
        except FileNotFoundError: #최초 실행의 경우 파일이 없음
            with open('config.json','w') as out_file:
                pages_to_fetch=100
                config={'pages_to_fetch':1}
                json.dump(config,out_file) #config값을 out_file에 저장
        self.update_daily_price(pages_to_fetch)

        tmnow=datetime.now()
        lastday=calendar.monthrange(tmnow.year,tmnow.month)[1] #해당 달의 마지막 일자 e.g.) 31
        if tmnow.month==12 and tmnow.day==lastday: #12월 마지막날인 경우 year+1 and 1/1 로 세팅
            tmnext=tmnow.replace(year=tmnow.year+1,month=1,day=1,
            hour=17,minute=0,second=0)

        elif tmnow.day==lastday:
            tmnext=tmnow.replace(month=tmnow.month+1,day=1,hour=17,minute=0,second=0)

        else:
            tmnext=tmnow.replace(day=tmnow.day+1, hour=17,minute=0,second=0)
        
        tmdiff=tmnext-tmnow
        secs=tmdiff.seconds

        t=Timer(secs, self.execute_daily)
        print("Waiting for next update({}).....".format(tmnext.strftime('%Y-%m-%d %H:%M')))
        t.start()


if __name__=='__main__':
    dbu=DBUpdater()
    dbu.execute_daily()