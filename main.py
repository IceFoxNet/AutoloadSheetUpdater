import os

while True:
    try:
        import yadisk, requests, gspread, time, pytz
        from datetime import datetime, timedelta
    except ImportError as e:
        package = e.msg.split()[-1][1:-1]
        os.system(f'python -m pip install {package}')
    else:
        break

def main(start: int, end: int, link: str, setup: dict):
    if start < 2: start = 2
    spreadsheet: gspread.spreadsheet.Spreadsheet = setup.get('AutoloadSheet')
    worksheet = spreadsheet.worksheet('Автовыгрузка Avito')
    avito_params: dict = setup.get('AvitoParams')
    yandex: yadisk.YaDisk = setup.get('YandexDisk')
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
        if item == '': continue
        identifier = identifiers[idx].value
        print(f'Работаем со строкой  {idx+1} (/Авито/{item}) из {len(data)}')
        if not begins[idx][0]:
            begins[idx] = [actual_date.strftime('%d.%m.%Y')]
            hundred_counter += 1
            if hundred_counter == 100:
                hundred_counter = 0
                actual_date += timedelta(days=1)
        try:
            files = list(yandex.listdir(f'/Авито/{item}'))[:10-len(link.split('|'))]
            if any(file.public_url is None for file in files):
                for file in files:
                    file.publish()
                files = list(yandex.listdir(f'/Авито/{item}'))[:10-len(link.split('|'))]
            results[idx] = [' | '.join(file.public_url.replace('yadi.sk', 'disk.yandex.ru') for file in files)]
            if link is not None:
                results[idx][0] = results[idx][0] + ' | ' + link
        except:
            results[idx] = [None]
            continue
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
    worksheet.update(results, f'H{start}:H{end}')
    worksheet.update(avito_id, f'A{start}:A{end}')
    worksheet.update(avito_status, f'B{start}:B{end}')
    time.sleep(120)
    headers = {
        'Authorization': f'Bearer {avito_token}'
    }
    app_script_runner('RENDER')
    time.sleep(60)
    response = requests.post('https://api.avito.ru/autoload/v1/upload', headers=headers)
    if not response.ok: raise SystemError(f'Ошибка при попытке запустить автовыгрузку ({response.status_code}) {response.json()}')

if __name__ == '__main__':
    from Setup.setup import setup
    main(3, 10_000, 'https://disk.yandex.ru/i/G_BtG2qVR5kn-g | https://disk.yandex.ru/i/v-C-CzkcpdBA5A', setup)
