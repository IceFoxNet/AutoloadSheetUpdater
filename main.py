from datetime import datetime, timedelta
import requests, gspread, time, pytz

from sqlalchemy import Column, String, UUID
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy import create_engine
from uuid import uuid4 as uuid
from sqlalchemy import and_, delete

Base = declarative_base()
class Media(Base):
    __tablename__ = 'media'
    id = Column(UUID, nullable=False, primary_key=True, comment='ID Созданного медиа')
    author_id = Column(UUID, nullable=False, comment='ID Приложения, создавшего медиа')
    name = Column(String, nullable=False, comment='Название файла')
    url = Column(String, comment='Прямая ссылка на медиа')
    author_ver = Column(String, comment='Версия приложения, создавшего медиа')
    resource_id = Column(String, nullable=False, comment='Идентификатор, по которому можно найти медиа')
    product_id = Column(String, nullable=False, comment='Идентификатор товара')
    description = Column(String, comment='Дополнительная информация о медиа')

class DBConnect:
    def __init__(self, appInfo: dict):
        self.login = appInfo.get('DBLogin')
        self.password = appInfo.get('DBPassword')
        self.appVer = appInfo.get('AppVer')
        self.id = appInfo.get('DBID')
        try:
            engine = create_engine(url=f"postgresql+psycopg2://{self.login}:{self.password}@scope-db-lego-invest-group.db-msk0.amvera.tech:5432/LEGOSystems", pool_pre_ping=True)
            Session = sessionmaker(bind=engine)
            self.session = Session()
        except Exception as e:
            raise SystemError(f'Ошибка авторизации в базе данных: {e}')

    def create_media(self, url: str, filename: str, resource_id: str, product_id: str, description: str | None):
        new_media = Media(
            id = uuid(),
            author_id = self.id,
            author_ver = self.appVer,
            resource_id = resource_id,
            product_id = product_id,
            url = url,
            name = filename,
            description = description
        )
        self.session.add(new_media)
        self.session.commit()

    def get_media_urls_by_resource_id(self, resource_id: str):
        results = self.session.query(Media.url).where(Media.resource_id == resource_id).all()
        return [res[0] for res in results]

    def is_actual_media_generated(self, resource_id: str):
        results = self.session.query(Media.author_ver).where(and_(Media.author_id == self.id, Media.resource_id == resource_id)).all()
        if len(results) == 0: return False
        return all(res[0] == self.appVer for res in results)

    def delete_media(self, resource_id: str, filename: str):
        media = self.session.query(Media).where(and_(Media.author_id == self.id, Media.resource_id == resource_id, Media.name == filename)).one_or_none()
        if media is not None:
            self.session.execute(delete(Media).where(Media.id == media.id))
    
    def close(self):
        self.session.close()

def main(start: int, end: int, link: str, setup: dict):
    if start < 2: start = 2
    spreadsheet: gspread.spreadsheet.Spreadsheet = setup.get('AutoloadSheet')
    dbconn = DBConnect(setup.get('AppInfo'))
    worksheet = spreadsheet.worksheet('Автовыгрузка Avito')
    avito_params: dict = setup.get('AvitoParams')
    app_script_runner = setup.get('AppScriptsRunner')
    response = requests.post('https://api.avito.ru/token', params=avito_params)
    response_data: dict = response.json()
    avito_token = response_data.get('access_token')
    data = list(filter(lambda x: x.value != '', worksheet.range(f'G{start}:G{end}')))
    results = list(map(lambda x: [x.value], worksheet.range(f'H{start}:H{end}')))
    avito_id = list(map(lambda x: [x.value], worksheet.range(f'A{start}:A{end}')))
    avito_status = list(map(lambda x: [x.value], worksheet.range(f'B{start}:B{end}')))
    begins = list(map(lambda x: [x.value], worksheet.range(f'I{start}:I{end}')))
    identifiers = worksheet.range(f'D{start}:D{end}')
    hundred_counter = 0
    actual_date = datetime.now(pytz.timezone('Europe/Moscow'))
    for idx in range(len(data)):
        item = data[idx].value
        if not item: continue
        identifier = identifiers[idx].value
        print(f'Работаем со строкой  {idx+1} (/Авито/{item}) из {len(data)}')
        if not begins[idx][0]:
            begins[idx] = [actual_date.strftime('%d.%m.%Y')]
            hundred_counter += 1
            if hundred_counter == 100:
                hundred_counter = 0
                actual_date += timedelta(days=1)
        media_links = dbconn.get_media_urls_by_resource_id(item)
        media_links = [x for x in dbconn.get_media_urls_by_resource_id(item) if x is not None]
        if media_links:
            results[idx][0] = ' | '.join(media_links) + ' | ' + link
        else:
            results[idx][0] = ''
        time.sleep(0.01)
    worksheet.update(results, f'H{start}:H{end}')
    worksheet.update(begins, f'I{start}:I{end}')
    time.sleep(120)
    headers = {
        'Authorization': f'Bearer {avito_token}'
    }
    app_script_runner('RENDER')
    time.sleep(120)
    response = requests.post('https://api.avito.ru/autoload/v1/upload', headers=headers)
    if not response.ok: raise SystemError(f'Ошибка при попытке запустить автовыгрузку ({response.status_code}) {response.json()}')
    time.sleep(60*60)
    for idx in range(len(data)):
        try:
            headers = {
                'Authorization': f'Bearer {avito_token}'
            }
            request_params = {
                'query': identifier
            }
            response = requests.get('https://api.avito.ru/autoload/v2/reports/items', headers=headers, params=request_params)
            response_data: dict = response.json()
            response_item: dict = response_data.get('items')[0]
            avito_id[idx] = [response_item.get('avito_id')]
            avito_status[idx] = [response_item.get('avito_status')]
        except:
            avito_id[idx] = [None]
            avito_status[idx] = [None]
            continue
    worksheet.update(avito_id, f'A{start}:A{end}')
    worksheet.update(avito_status, f'B{start}:B{end}')
    dbconn.close()

if __name__ == '__main__':
    from Setup.setup import setup
    main(3, 5, 'https://disk.yandex.ru/i/G_BtG2qVR5kn-g | https://disk.yandex.ru/i/v-C-CzkcpdBA5A', setup)
