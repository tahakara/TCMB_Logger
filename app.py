import os
import logging
import mysql.connector
import pymysql
import requests
import json
import xmltodict
import schedule
import time
from datetime import datetime, timedelta, date
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/tcmb_logger.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class TCMBDatabaseManager:
    def __init__(self):
        self.db_config = self.get_mysql_credentials()
        self.utc_offset = int(os.getenv('UTC_OFFSET', 3))
        
    def get_mysql_credentials(self):
        """Environment değişkenlerinden MySQL bağlantı bilgilerini çeker"""
        credentials = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'port': int(os.getenv('MYSQL_PORT', 3306)),
            'user': os.getenv('MYSQL_USER'),
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': os.getenv('MYSQL_DATABASE', 'tcmb')
        }
        
        if not credentials['user'] or not credentials['password']:
            logger.error("MySQL kullanıcı adı ve şifresi environment değişkenlerinde tanımlanmalıdır")
            raise ValueError("MYSQL_USER ve MYSQL_PASSWORD environment değişkenleri gerekli")
        
        logger.info(f"MySQL bağlantı bilgileri environment'dan başarıyla okundu - Host: {credentials['host']}")
        return credentials

    def setup_database(self):
        """Database ve tabloları oluşturur"""
        logger.info("TCMB Database kurulum işlemi başlatılıyor...")
        
        try:
            # MySQL sunucusuna bağlan (database olmadan)
            connection = mysql.connector.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            
            cursor = connection.cursor()
            
            # Database oluştur
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_config['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            logger.info(f"Database '{self.db_config['database']}' kontrol edildi/oluşturuldu")
            
            # Database'i seç
            cursor.execute(f"USE {self.db_config['database']}")
            
            # Analytical Balance Sheet tablosu oluştur
            create_analytical_table = """
            CREATE TABLE IF NOT EXISTS ANALYTICAL_BALANCE_SHEET (
                id INT AUTO_INCREMENT PRIMARY KEY,
                UNIXTIME BIGINT,
                _DATE DATE,
                TP_AB_A01 DECIMAL(20,8),
                TP_AB_A02 DECIMAL(20,8),
                TP_AB_A03 DECIMAL(20,8),
                TP_AB_A04 DECIMAL(20,8),
                TP_AB_A05 DECIMAL(20,8),
                TP_AB_A051 DECIMAL(20,8),
                TP_AB_A052 DECIMAL(20,8),
                TP_AB_A053 DECIMAL(20,8),
                TP_AB_A054 DECIMAL(20,8),
                TP_AB_A06 DECIMAL(20,8),
                TP_AB_A061 DECIMAL(20,8),
                TP_AB_A07 DECIMAL(20,8),
                TP_AB_A08 DECIMAL(20,8),
                TP_AB_A081 DECIMAL(20,8),
                TP_AB_A09 DECIMAL(20,8),
                TP_AB_A10 DECIMAL(20,8),
                TP_AB_A11 DECIMAL(20,8),
                TP_AB_A12 DECIMAL(20,8),
                TP_AB_A13 DECIMAL(20,8),
                TP_AB_A14 DECIMAL(20,8),
                TP_AB_A15 DECIMAL(20,8),
                TP_AB_A16 DECIMAL(20,8),
                TP_AB_A17 DECIMAL(20,8),
                TP_AB_A18 DECIMAL(20,8),
                TP_AB_A19 DECIMAL(20,8),
                TP_AB_A20 DECIMAL(20,8),
                TP_AB_A21 DECIMAL(20,8),
                TP_AB_A22 DECIMAL(20,8),
                TP_AB_A23 DECIMAL(20,8),
                TP_AB_A24 DECIMAL(20,8),
                TP_AB_A25 DECIMAL(20,8),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_date (_DATE)
            )
            """
            cursor.execute(create_analytical_table)
            logger.info("ANALYTICAL_BALANCE_SHEET tablosu kontrol edildi/oluşturuldu")
            
            # Exchange Rates tablosu oluştur
            create_exchange_table = """
            CREATE TABLE IF NOT EXISTS INDICATIVE_EXCHANGE_RATES (
                id INT AUTO_INCREMENT PRIMARY KEY,
                BULETIN_NUM VARCHAR(50),
                _DATE DATE,
                CURRENCY_COD VARCHAR(10),
                CURRENCY_NAME VARCHAR(100),
                FX_BUY DECIMAL(10,4),
                FX_SELL DECIMAL(10,4),
                BANKNOTE_BUY DECIMAL(10,4),
                BANKNOTE_SELL DECIMAL(10,4),
                CROSSRATE_USD DECIMAL(10,4),
                UNIT INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_currency_date (_DATE, CURRENCY_COD)
            )
            """
            cursor.execute(create_exchange_table)
            logger.info("INDICATIVE_EXCHANGE_RATES tablosu kontrol edildi/oluşturuldu")
            
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info("Database kurulum işlemi başarıyla tamamlandı")
            return True
            
        except Exception as e:
            logger.error(f"Database kurulum işleminde hata: {e}")
            return False

    def get_connection(self):
        """PyMySQL bağlantısı döndürür (SSL olmadan)"""
        try:
            connection = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info("PyMySQL bağlantısı başarıyla kuruldu")
            return connection
        except Exception as e:
            logger.error(f"PyMySQL bağlantı hatası: {e}")
            raise

    def get_utc_plus_3_time(self, days_offset=0):
        """UTC+3 zamanını döndürür"""
        return datetime.now() + timedelta(hours=self.utc_offset, days=days_offset)

    def get_analytical_balance_sheet(self, target_date=None):
        """Analitik bilanço verilerini çeker"""
        if target_date is None:
            analytic_day = self.get_utc_plus_3_time(-2)
        else:
            analytic_day = target_date
            
        analytic_day_str = analytic_day.strftime("%d-%m-%Y")
        
        logger.info(f"Analitik bilanço verileri çekiliyor - Tarih: {analytic_day_str}")
        
        analytic_header = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
            'Origin': 'https://evds2.tcmb.gov.tr',
            'Referer': 'https://evds2.tcmb.gov.tr/index.php?/evds/portlet/IScH+A8YwOo=/tr'
        }

        form_data = {
            "thousand":"1",
            "decimal":"8",
            "frequency":"WORKDAY",
            "aggregationType":"last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last#last",
            "formula":"0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0",
            "graphicType":"0",
            "skip":"0",
            "take":"12",
            "toExcel":"true",
            "select":"TP_AB_A01-0#TP_AB_A02-0#TP_AB_A03-0#TP_AB_A04-0#TP_AB_A05-0#TP_AB_A051-0#TP_AB_A052-0#TP_AB_A053-0#TP_AB_A054-0#TP_AB_A06-0#TP_AB_A061-0#TP_AB_A07-0#TP_AB_A08-0#TP_AB_A081-0#TP_AB_A09-0#TP_AB_A10-0#TP_AB_A11-0#TP_AB_A12-0#TP_AB_A13-0#TP_AB_A14-0#TP_AB_A15-0#TP_AB_A16-0#TP_AB_A17-0#TP_AB_A18-0#TP_AB_A19-0#TP_AB_A20-0#TP_AB_A21-0#TP_AB_A22-0#TP_AB_A23-0#TP_AB_A24-0#TP_AB_A25-0",
            "startDate":f"{analytic_day_str}",
            "endDate":f"{analytic_day_str}",
            "obsCountEnabled":"",
            "obsCount":"",
            "categories":"5932",
            "mongoAdresses":"evds",
            "userId":"",
            "datagroupString":"bie_abanlbil",
            "dateFormatValue":"dd-mm-yyyy",
            "customFormula":"null",
            "excludedSeries":"null",
            "sort":"Tarih#true",
            "orderby":"Tarih desc"
        }
        
        try:
            response = requests.post(
                "https://evds2.tcmb.gov.tr/EVDSServlet",
                headers=analytic_header,
                data=form_data,
                timeout=30
            )

            if response.status_code == 200:
                data = json.loads(response.content.decode('utf-8'))
                
                if data.get('items') and len(data['items']) > 0:
                    item_list = []
                    value_list = []
                    
                    for key, value in data['items'][0].items():
                        if key == "UNIXTIME":
                            item_list.append("UNIXTIME")
                            value_list.append(value["$numberLong"])
                        elif key == "Tarih":
                            item_list.append("_DATE")
                            editable_date = value.split("-")
                            edited_date = f"{editable_date[2]}-{editable_date[1]}-{editable_date[0]}"
                            value_list.append(edited_date)
                        else:
                            item_list.append(key)
                            value_list.append(value)
                    
                    item_list_str = str(tuple(item_list)).replace("'", "`")
                    value_list_str = str(tuple(value_list)).replace("'", '"')
                    query = f'INSERT IGNORE INTO ANALYTICAL_BALANCE_SHEET {item_list_str} VALUES {value_list_str};'
                    
                    logger.info(f"Analitik bilanço verileri başarıyla çekildi - Tarih: {analytic_day_str}")
                    return query
                else:
                    logger.warning(f"Analitik bilanço verisi bulunamadı - Tarih: {analytic_day_str}")
                    return None
            else:
                logger.error(f"Analitik bilanço API hatası - Status: {response.status_code}, Tarih: {analytic_day_str}")
                return None
                
        except Exception as e:
            logger.error(f"Analitik bilanço çekme hatası: {e}")
            return None

    def float_or_none(self, x):
        """String'i float'a çevirir, hata durumunda None döner"""
        try:
            if not x or x == 'None' or x == '':
                return None
            
            splited = x.split('.')
            if len(splited) == 1:
                x = x[0] if isinstance(splited, list) else x
                index = len(x) - 4
                new_string = x[:index] + '.' + x[index:]
                return float(new_string)
            else:
                return float(x)
        except:
            return None

    def page_no(self, year, month, day):
        """Sayfa numarası hesaplar"""
        lorem1 = date(int(year), int(month), int(day))
        lorem2 = date(int(year), 1, 1)
        lorem3 = (lorem1 - lorem2).days
        return f"{year}/{lorem3}"

    def get_indicative_exchange_rates(self, target_date=None):
        """Döviz kurları verilerini çeker"""
        if target_date is None:
            exchange_day = self.get_utc_plus_3_time(-1)
        else:
            exchange_day = target_date
            
        exchange_day_str = exchange_day.strftime("%d-%m-%Y")
        exchange_day_parts = exchange_day_str.split("-")
        year = exchange_day_parts[2]
        month = exchange_day_parts[1]
        day = exchange_day_parts[0]
        param1 = year + month
        param2 = day + month + year

        logger.info(f"Döviz kurları çekiliyor - Tarih: {exchange_day_str}")

        exchange_header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4434.0 Safari/537.36 Edg/91.0.866.0"
        }

        try:
            response = requests.get(
                f"https://www.tcmb.gov.tr/kurlar/{param1}/{param2}.xml",
                headers=exchange_header,
                timeout=30
            )

            if response.status_code == 200:
                content = response.content.decode('latin-1')
                data = xmltodict.parse(content)

                bulletin = data['Tarih_Date'].get('@Bulten_No', self.page_no(year, month, day))
                raw_date = data['Tarih_Date']['@Tarih'].split('.')
                _date = f"{raw_date[2]}-{raw_date[1]}-{raw_date[0]}"

                values_list = []
                
                for currency in data['Tarih_Date']['Currency']:
                    currency_code = currency['@CurrencyCode']
                    currency_name = currency['CurrencyName']
                    fx_buying = self.float_or_none(currency.get('ForexBuying'))
                    fx_selling = self.float_or_none(currency.get('ForexSelling'))
                    banknote_buying = self.float_or_none(currency.get('BanknoteBuying'))
                    banknote_selling = self.float_or_none(currency.get('BanknoteSelling'))
                    cross_rate_usd = self.float_or_none(currency.get('CrossRateUSD'))
                    unit = 1 if currency.get('Unit') in ['None', None, '', '1'] else int(currency.get('Unit', 1))

                    value_tuple = f"('{bulletin}', '{_date}', '{currency_code}', '{currency_name}', {fx_buying}, {fx_selling}, {banknote_buying}, {banknote_selling}, {cross_rate_usd}, {unit})"
                    values_list.append(value_tuple.replace('None', 'NULL'))

                query = f"INSERT IGNORE INTO INDICATIVE_EXCHANGE_RATES (BULETIN_NUM, _DATE, CURRENCY_COD, CURRENCY_NAME, FX_BUY, FX_SELL, BANKNOTE_BUY, BANKNOTE_SELL, CROSSRATE_USD, UNIT) VALUES {', '.join(values_list)};"
                
                logger.info(f"Döviz kurları başarıyla çekildi - Tarih: {exchange_day_str}")
                return query
            else:
                logger.error(f"Döviz kurları API hatası - Status: {response.status_code}, Tarih: {exchange_day_str}")
                return None
                
        except Exception as e:
            logger.error(f"Döviz kurları çekme hatası: {e}")
            return None

    def collect_data_for_date(self, target_date):
        """Belirli bir tarih için veri toplar"""
        logger.info(f"Veri toplama başlatılıyor - Tarih: {target_date.strftime('%Y-%m-%d')}")
        
        connection = self.get_connection()
        cursor = connection.cursor()
        
        try:
            # Analitik bilanço verilerini çek
            analytic_query = self.get_analytical_balance_sheet(target_date)
            if analytic_query:
                cursor.execute(analytic_query)
                connection.commit()
                logger.info(f"Analitik bilanço verileri kaydedildi - Tarih: {target_date.strftime('%Y-%m-%d')}")
            else:
                logger.warning(f"Analitik bilanço verisi yok - Tarih: {target_date.strftime('%Y-%m-%d')}")

            # Döviz kurları verilerini çek
            exchange_query = self.get_indicative_exchange_rates(target_date)
            if exchange_query:
                cursor.execute(exchange_query)
                connection.commit()
                logger.info(f"Döviz kurları kaydedildi - Tarih: {target_date.strftime('%Y-%m-%d')}")
            else:
                logger.warning(f"Döviz kurları verisi yok - Tarih: {target_date.strftime('%Y-%m-%d')}")

        except Exception as e:
            logger.error(f"Veri toplama hatası - Tarih: {target_date.strftime('%Y-%m-%d')}, Hata: {e}")
        finally:
            cursor.close()
            connection.close()

    def collect_historical_data(self):
        """Geçmiş verileri toplar"""
        start_date_str = os.getenv('START_DATE', '2023-01-01')
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = self.get_utc_plus_3_time(-1)  # Dün
        
        logger.info(f"Geçmiş veri toplama başlatılıyor - {start_date.strftime('%Y-%m-%d')} ile {end_date.strftime('%Y-%m-%d')} arası")
        
        current_date = start_date
        while current_date <= end_date:
            # Hafta sonu kontrolü (TCMB verileri genelde hafta içi)
            if current_date.weekday() < 5:  # 0-4 hafta içi, 5-6 hafta sonu
                self.collect_data_for_date(current_date)
                time.sleep(2)  # API'yi zorlamak için bekleme
            current_date += timedelta(days=1)
        
        logger.info("Geçmiş veri toplama tamamlandı")

    def daily_data_collection(self):
        """Günlük veri toplama işlemi"""
        logger.info("Günlük veri toplama başlatılıyor")
        yesterday = self.get_utc_plus_3_time(-1)
        
        # Hafta sonu kontrolü
        if yesterday.weekday() < 5:  # Hafta içi
            self.collect_data_for_date(yesterday)
            logger.info(f"Günlük veri toplama tamamlandı - {yesterday.strftime('%Y-%m-%d')}")
        else:
            logger.info(f"Hafta sonu olduğu için veri toplama atlandı - {yesterday.strftime('%Y-%m-%d')}")

def main():
    """Ana fonksiyon"""
    logger.info("TCMB Logger uygulaması başlatılıyor...")
    
    db_manager = TCMBDatabaseManager()
    
    # Database kurulumu
    if not db_manager.setup_database():
        logger.error("Database kurulum başarısız, uygulama sonlandırılıyor")
        return
    
    # İlk çalıştırmada geçmiş verileri topla
    logger.info("Geçmiş veriler kontrol ediliyor...")
    db_manager.collect_historical_data()  # İlk çalıştırmada aktif edin
    
    # Günlük veri toplama işlemini zamanla (UTC+3 saat 03:00'da)
    schedule.every().day.at("03:00").do(db_manager.daily_data_collection)
    logger.info("Günlük veri toplama zamanlandı (UTC+3 03:00)")
    
    # İlk günlük toplama işlemini hemen çalıştır
    db_manager.daily_data_collection()
    
    logger.info("TCMB Logger çalışıyor... (Ctrl+C ile durdurun)")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Her dakika kontrol et
    except KeyboardInterrupt:
        logger.info("TCMB Logger durduruldu")

if __name__ == "__main__":
    main()
