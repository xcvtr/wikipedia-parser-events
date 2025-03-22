import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def clean_disaster_type(url):
    """Очищает и форматирует тип катастрофы из URL"""
    # Удаляем префикс и суффикс
    disaster_type = url.replace('https://en.wikipedia.org/wiki/List_of_', '').replace('s', '')
    # Убираем специальные символы и приводим к нижнему регистру
    disaster_type = re.sub(r'[^a-zA-Z0-9\s]', '', disaster_type).lower()
    
    # Маппинг для более читаемых названий
    type_mapping = {
        'diaterinswedenbydeathtoll': 'disaster_in_sweden',
        'diaterinbangladehbydeathtoll': 'disaster_in_bangladesh',
        'maritimediater': 'maritime_disaster',
        'accidentanddiaterbydeathtoll': 'accident_and_disaster',
        'naturaldiaterbydeathtoll': 'natural_disaster'
    }
    
    return type_mapping.get(disaster_type, disaster_type)

def clean_text(text):
    """Очищает текст от лишних пробелов и переносов строк"""
    if not text:
        return "Unknown"
    return ' '.join(text.strip().split())

def is_year(text):
    """Проверяет, содержит ли текст год"""
    # Ищем 4 цифры подряд в диапазоне 1000-2100
    years = re.findall(r'\b(1\d{3}|20[0-2]\d)\b', str(text))
    return bool(years)

def is_date(text):
    """Проверяет, является ли текст датой"""
    # Проверяем форматы даты: "YYYY Month DD" или "Month DD, YYYY"
    date_patterns = [
        r'\b(1\d{3}|20[0-2]\d)\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}\b',
        r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+(1\d{3}|20[0-2]\d)\b'
    ]
    return any(re.search(pattern, str(text)) for pattern in date_patterns)

def is_death_toll(text):
    """Проверяет, является ли текст количеством жертв"""
    text = str(text).strip()
    
    # Если текст содержит месяц, это вероятно дата
    months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 
              'august', 'september', 'october', 'november', 'december']
    if any(month in text.lower() for month in months):
        return False
    
    # Проверяем форматы количества жертв
    death_patterns = [
        # Простое число: "123"
        r'^\d+$',
        # Число с запятыми: "1,234"
        r'^\d{1,3}(,\d{3})*$',
        # Диапазон: "1,234-5,678" или "100-200"
        r'^\d{1,3}(,\d{3})*-\d{1,3}(,\d{3})*$',
        r'^\d+-\d+$',
        # Приблизительное количество: "~100", "c. 200"
        r'^[~c\. ]*\d+$',
        # Более/менее: ">100", "<200"
        r'^[<>]\s*\d+$'
    ]
    
    # Проверяем каждый паттерн
    for pattern in death_patterns:
        if re.match(pattern, text):
            # Дополнительная проверка: если число похоже на год (1800-2024),
            # это вероятно дата
            numbers = re.findall(r'\d+', text)
            if numbers and all(1800 <= int(n) <= 2024 for n in numbers):
                return False
            return True
            
    return False

def get_column_type(header):
    """Определяет тип столбца по его заголовку"""
    header = str(header).lower().strip()
    
    # Маппинг заголовков на типы столбцов
    header_mapping = {
        'death': 'death_toll',
        'deaths': 'death_toll',
        'casualties': 'death_toll',
        'fatalities': 'death_toll',
        'killed': 'death_toll',
        'toll': 'death_toll',
        'dead': 'death_toll',
        
        'date': 'date',
        'year': 'date',
        'when': 'date',
        'time': 'date',
        
        'location': 'location',
        'place': 'location',
        'where': 'location',
        'area': 'location',
        'region': 'location',
        'country': 'location',
        'site': 'location',
        
        'event': 'event',
        'incident': 'event',
        'disaster': 'event',
        'description': 'event',
        'name': 'event',
        
        'notes': 'details',
        'details': 'details',
        'description': 'details',
        'comments': 'details',
        'additional': 'details'
    }
    
    # Проверяем каждое слово в заголовке
    for word in header.split():
        if word in header_mapping:
            return header_mapping[word]
    
    return None

def determine_event_type(event_name, details):
    """Определяет тип и подтип события на основе его описания"""
    event_text = f"{event_name} {details}".lower()
    
    # Определяем основной тип (nature или technological)
    nature_keywords = ['earthquake', 'tsunami', 'flood', 'hurricane', 'tornado', 'storm', 
                      'volcano', 'avalanche', 'landslide', 'drought', 'famine', 'pandemic',
                      'epidemic', 'wildfire', 'cyclone', 'typhoon', 'blizzard', 'heat wave',
                      'cold wave', 'frost', 'hail', 'snow', 'rain', 'natural']
    
    tech_keywords = ['explosion', 'fire', 'crash', 'collision', 'derailment', 'sinking',
                    'crush', 'stampede', 'terrorist', 'terrorism', 'bombing', 'shooting',
                    'massacre', 'industrial', 'mining', 'chemical', 'nuclear', 'radiation',
                    'leak', 'spill', 'accident', 'disaster', 'man-made']
    
    # Определяем основной тип
    event_type = 'technological'
    for keyword in nature_keywords:
        if keyword in event_text:
            event_type = 'nature'
            break
    
    # Определяем подтип
    subtype = 'other'
    
    # Природные катастрофы
    if event_type == 'nature':
        if any(word in event_text for word in ['earthquake', 'quake']):
            subtype = 'earthquake'
        elif any(word in event_text for word in ['tsunami', 'tidal wave']):
            subtype = 'tsunami'
        elif any(word in event_text for word in ['flood', 'flooding', 'inundation']):
            subtype = 'flood'
        elif any(word in event_text for word in ['hurricane', 'cyclone', 'typhoon']):
            subtype = 'tropical_cyclone'
        elif any(word in event_text for word in ['tornado', 'twister']):
            subtype = 'tornado'
        elif any(word in event_text for word in ['volcano', 'eruption', 'volcanic']):
            subtype = 'volcanic_eruption'
        elif any(word in event_text for word in ['avalanche', 'snowslide']):
            subtype = 'avalanche'
        elif any(word in event_text for word in ['landslide', 'mudslide', 'rockslide']):
            subtype = 'landslide'
        elif any(word in event_text for word in ['drought', 'famine']):
            subtype = 'drought'
        elif any(word in event_text for word in ['pandemic', 'epidemic', 'plague']):
            subtype = 'pandemic'
        elif any(word in event_text for word in ['wildfire', 'forest fire', 'bushfire']):
            subtype = 'wildfire'
        elif any(word in event_text for word in ['blizzard', 'snowstorm']):
            subtype = 'blizzard'
        elif any(word in event_text for word in ['heat wave', 'heatwave']):
            subtype = 'heat_wave'
        elif any(word in event_text for word in ['cold wave', 'coldwave', 'frost']):
            subtype = 'cold_wave'
    
    # Техногенные катастрофы
    else:
        if any(word in event_text for word in ['explosion', 'blast']):
            subtype = 'explosion'
        elif any(word in event_text for word in ['fire', 'blaze', 'conflagration']):
            subtype = 'fire'
        elif any(word in event_text for word in ['crash', 'collision', 'accident']):
            subtype = 'transport_accident'
        elif any(word in event_text for word in ['derailment', 'train']):
            subtype = 'train_accident'
        elif any(word in event_text for word in ['sinking', 'shipwreck', 'ship']):
            subtype = 'maritime_accident'
        elif any(word in event_text for word in ['crush', 'stampede', 'crowd']):
            subtype = 'crowd_crush'
        elif any(word in event_text for word in ['terrorist', 'terrorism', 'bombing', 'bomb']):
            subtype = 'terrorism'
        elif any(word in event_text for word in ['shooting', 'massacre', 'mass killing']):
            subtype = 'mass_shooting'
        elif any(word in event_text for word in ['industrial', 'factory']):
            subtype = 'industrial_accident'
        elif any(word in event_text for word in ['mining', 'mine']):
            subtype = 'mining_accident'
        elif any(word in event_text for word in ['chemical', 'toxic', 'poison']):
            subtype = 'chemical_accident'
        elif any(word in event_text for word in ['nuclear', 'radiation', 'radioactive']):
            subtype = 'nuclear_accident'
        elif any(word in event_text for word in ['leak', 'spill', 'contamination']):
            subtype = 'environmental_disaster'
    
    return event_type, subtype

def format_date(date_str):
    if not date_str:
        return "Unknown"
    
    # Словарь месяцев
    months = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04',
        'May': '05', 'June': '06', 'July': '07', 'August': '08',
        'September': '09', 'October': '10', 'November': '11', 'December': '12',
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
        'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09',
        'Oct': '10', 'Nov': '11', 'Dec': '12'
    }
    
    # Паттерны для различных форматов дат
    patterns = [
        # YYYY-MM-DD
        (r'(\d{4})-(\d{2})-(\d{2})', lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}"),
        # DD Month YYYY
        (r'(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})',
         lambda m: f"{m.group(3)}-{months.get(m.group(2), '01')}-{m.group(1).zfill(2)}"),
        # Month DD, YYYY
        (r'([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,\s*(\d{4})',
         lambda m: f"{m.group(3)}-{months.get(m.group(1), '01')}-{m.group(2).zfill(2)}"),
        # YYYY Month DD
        (r'(\d{4})\s+([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?',
         lambda m: f"{m.group(1)}-{months.get(m.group(2), '01')}-{m.group(3).zfill(2)}"),
        # YYYY
        (r'(\d{4})', lambda m: f"{m.group(1)}-01-01")
    ]
    
    # Обработка диапазонов дат
    if '–' in date_str or '-' in date_str:
        parts = re.split(r'[–-]', date_str)
        if len(parts) == 2:
            start_date = parts[0].strip()
            for pattern, formatter in patterns:
                match = re.search(pattern, start_date)
                if match:
                    return formatter(match)
    
    # Поиск даты по паттернам
    for pattern, formatter in patterns:
        match = re.search(pattern, date_str)
        if match:
            return formatter(match)
    
    return "Unknown"

def extract_city(location, event_name, details):
    if not location and not event_name and not details:
        return "Unknown"
    
    text = f"{location} {event_name} {details}"
    
    # Список городов и их вариаций
    cities = {
        "London": ["London", "Blackfriars, London"],
        "Glasgow": ["Glasgow", "Glasgow, Scotland"],
        "Manchester": ["Manchester"],
        "Dublin": ["Dublin", "Dublin, Ireland"],
        "Edinburgh": ["Edinburgh", "Edinburgh, Scotland"],
        "Belfast": ["Belfast", "Belfast, Northern Ireland"],
        "Liverpool": ["Liverpool"],
        "Birmingham": ["Birmingham"],
        "Leeds": ["Leeds"],
        "Newcastle": ["Newcastle"],
        "Plymouth": ["Plymouth"],
        "Cardiff": ["Cardiff"],
        "Bristol": ["Bristol"],
        "Oxford": ["Oxford"],
        "Cambridge": ["Cambridge"],
        "Aberdeen": ["Aberdeen"],
        "Dundee": ["Dundee"]
    }
    
    # Поиск города в тексте
    for city, variations in cities.items():
        for variation in variations:
            if variation in text:
                return city
    
    # Извлечение города из текста с помощью регулярных выражений
    city_patterns = [
        r'in ([A-Z][a-z]+)(?:,|\s|$)',
        r'at ([A-Z][a-z]+)(?:,|\s|$)',
        r'near ([A-Z][a-z]+)(?:,|\s|$)',
        r'^([A-Z][a-z]+)(?:,|\s|$)'
    ]
    
    for pattern in city_patterns:
        match = re.search(pattern, text)
        if match:
            city = match.group(1)
            # Проверяем, что это не страна или регион
            if city not in ["England", "Scotland", "Wales", "Ireland", "Britain", "UK"]:
                return city
    
    return "Unknown"

def extract_country_from_url(url):
    """Извлекает название страны из URL"""
    # Ищем паттерн "List_of_disasters_in_X" или "List_of_X_disasters"
    patterns = [
        r'List_of_disasters_in_([A-Za-z_]+)_by',
        r'List_of_([A-Za-z_]+)_disasters_by',
        r'List_of_([A-Za-z_]+)_disasters$'
    ]
    
    # Словарь для нормализации названий стран
    country_mapping = {
        'the_United_States': 'United States',
        'the_United_Kingdom': 'United Kingdom',
        'Great_Britain': 'United Kingdom',
        'UK': 'United Kingdom',
        'US': 'United States',
        'USA': 'United States',
        'Romania': 'Romania',
        'Canada': 'Canada',
        'Australia': 'Australia',
        'New_Zealand': 'New Zealand',
        'India': 'India',
        'China': 'China',
        'Japan': 'Japan',
        'Russia': 'Russia',
        'France': 'France',
        'Germany': 'Germany',
        'Italy': 'Italy',
        'Spain': 'Spain',
        'Sweden': 'Sweden',
        'Norway': 'Norway',
        'Denmark': 'Denmark',
        'Finland': 'Finland',
        'Poland': 'Poland',
        'Czech': 'Czech Republic',
        'Slovakia': 'Slovakia',
        'Hungary': 'Hungary',
        'Austria': 'Austria',
        'Switzerland': 'Switzerland',
        'Netherlands': 'Netherlands',
        'Belgium': 'Belgium',
        'Greece': 'Greece',
        'Turkey': 'Turkey',
        'Iran': 'Iran',
        'Iraq': 'Iraq',
        'Saudi_Arabia': 'Saudi Arabia',
        'Egypt': 'Egypt',
        'South_Africa': 'South Africa',
        'Brazil': 'Brazil',
        'Argentina': 'Argentina',
        'Mexico': 'Mexico'
    }
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            country = match.group(1).replace('_', ' ')
            # Проверяем, что это действительно страна
            if country not in ['natural', 'environmental', 'maritime', 'nuclear', 'industrial']:
                # Проверяем маппинг стран
                for key, value in country_mapping.items():
                    if key.lower() == country.lower().replace(' ', '_'):
                        return value
                return country
    return None

def determine_country(location, event_name, details, url=None):
    """Определяет страну события из различных источников"""
    if not location and not event_name and not details and not url:
        return "Unknown"
    
    text = f"{location} {event_name} {details}"
    
    # Словарь стран и их вариаций
    countries = {
        "United Kingdom": [
            "UK", "U.K.", "Britain", "Great Britain", "England", "Scotland", "Wales",
            "Northern Ireland", "Glasgow, Scotland", "Edinburgh, Scotland",
            "London, England", "Cardiff, Wales", "Belfast, Northern Ireland",
            "County Durham", "Yorkshire", "Lancashire", "Cornwall", "Devon",
            "Glamorganshire", "Monmouthshire", "Staffordshire", "Derbyshire",
            "Isle of Man", "Isles of Scilly", "Shetland", "Thames"
        ],
        "Ireland": [
            "Ireland", "Dublin", "County Galway", "County Donegal", "Bantry Bay",
            "Republic of Ireland"
        ],
        "France": [
            "France", "Beauvais", "Brittany", "Aarsele"
        ],
        "United States": [
            "USA", "U.S.", "U.S.A.", "United States", "America"
        ],
        "Australia": [
            "Australia", "Victoria", "Tasmania"
        ],
        "Austria": [
            "Austria", "Innsbruck"
        ],
        "Belgium": [
            "Belgium", "Aarsele"
        ],
        "Germany": [
            "Germany", "Bavaria"
        ],
        "Japan": [
            "Japan"
        ]
    }
    
    # Поиск страны в тексте
    for country, variations in countries.items():
        for variation in variations:
            if variation in text:
                return country
    
    # Если страна не найдена в тексте, пытаемся извлечь из URL
    if url:
        country_from_url = extract_country_from_url(url)
        if country_from_url:
            return country_from_url
    
    return "Unknown"

def create_short_event_name(event_name, details):
    """Создает краткое название события на основе полного описания"""
    if not event_name or event_name == "Unknown":
        return "Unknown"
    
    # Если название уже короткое (менее 50 символов), оставляем его как есть
    if len(event_name) < 50:
        return event_name
    
    # Ищем ключевые слова в названии
    keywords = ['disaster', 'accident', 'incident', 'explosion', 'fire', 'crash', 
                'sinking', 'collision', 'derailment', 'flood', 'earthquake', 'tsunami']
    
    for keyword in keywords:
        if keyword in event_name.lower():
            # Берем часть текста до ключевого слова
            parts = event_name.lower().split(keyword)
            if parts[0]:
                return parts[0].strip().title()
    
    # Если не нашли ключевых слов, берем первые 50 символов
    return event_name[:50].strip()

def extract_links_from_text(soup):
    """Извлекает ссылки на катастрофы из текста страницы"""
    events = []
    
    # Находим все параграфы с текстом
    paragraphs = soup.find_all('p')
    
    for p in paragraphs:
        # Ищем ссылки в параграфе
        links = p.find_all('a')
        
        for link in links:
            href = link.get('href')
            if not href or not href.startswith('/wiki/'):
                continue
                
            # Получаем текст ссылки и следующий текст
            event_name = link.text.strip()
            next_text = link.next_sibling
            details = ""
            
            # Собираем текст до следующей ссылки или конца параграфа
            while next_text and not isinstance(next_text, BeautifulSoup):
                if isinstance(next_text, str):
                    details += next_text.strip() + " "
                next_text = next_text.next_sibling
            
            # Если нашли событие
            if event_name and any(keyword in event_name.lower() for keyword in 
                               ['disaster', 'accident', 'incident', 'explosion', 'fire', 'crash']):
                event_data = {
                    'Death toll': "Unknown",
                    'Event': create_short_event_name(event_name, details),
                    'City': "Unknown",
                    'Country': "Unknown",
                    'Date': "Unknown",
                    'Details': details.strip(),
                    'Event Type': "Unknown",
                    'Event Subtype': "Unknown",
                    'URL': f"https://en.wikipedia.org{href}"
                }
                
                # Определяем тип и подтип события
                event_type, subtype = determine_event_type(event_name, details)
                event_data['Event Type'] = event_type
                event_data['Event Subtype'] = subtype
                
                # Извлекаем город и страну
                event_data['City'] = extract_city("", event_name, details)
                event_data['Country'] = determine_country("", event_name, details)
                
                events.append(event_data)
    
    return events

def parse_page(url):
    """Парсит страницу с катастрофами"""
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Находим все таблицы на странице
        tables = soup.find_all('table', {'class': 'wikitable'})
        
        all_events = []
        
        if tables:
            # Парсим таблицы
            disaster_type = clean_disaster_type(url)
            for table in tables:
                events = parse_table(table, disaster_type)
                for event in events:
                    event['URL'] = url
                    # Если страна не определена, пытаемся определить её из URL
                    if event['Country'] == "Unknown":
                        country = extract_country_from_url(url)
                        if country:
                            event['Country'] = country
                all_events.extend(events)
        else:
            # Парсим ссылки из текста
            events = extract_links_from_text(soup)
            for event in events:
                # Если страна не определена, пытаемся определить её из URL
                if event['Country'] == "Unknown":
                    country = extract_country_from_url(url)
                    if country:
                        event['Country'] = country
            all_events.extend(events)
        
        return all_events
    
    except Exception as e:
        print(f"Ошибка при парсинге {url}: {str(e)}")
        return []

def parse_table(table, disaster_type):
    """Парсит таблицу и возвращает список событий"""
    events = []
    
    # Получаем заголовки
    headers = []
    header_row = table.find('tr')
    if header_row:
        headers = [get_column_type(th.text) for th in header_row.find_all(['th', 'td'])]
    
    if not headers or all(h is None for h in headers):
        print(f"Предупреждение: Не удалось определить заголовки таблицы")
        return []
    
    # Создаем маппинг индексов столбцов
    column_indices = {
        'death_toll': None,
        'date': None,
        'location': None,
        'event': None,
        'details': None
    }
    
    for idx, header_type in enumerate(headers):
        if header_type and column_indices[header_type] is None:
            column_indices[header_type] = idx
    
    # Если не нашли столбец с количеством жертв, пытаемся определить его по содержимому
    if column_indices['death_toll'] is None:
        first_data_row = table.find_all('tr')[1]
        if first_data_row:
            cols = first_data_row.find_all(['td', 'th'])
            for idx, col in enumerate(cols):
                if is_death_toll(col.text) and not is_date(col.text):
                    column_indices['death_toll'] = idx
                    break
    
    # Парсим строки таблицы
    for row in table.find_all('tr')[1:]:
        cols = row.find_all(['td', 'th'])
        if len(cols) >= len(headers):
            event_data = {
                'Death toll': "Unknown",
                'Event': "Unknown",
                'City': "Unknown",
                'Country': "Unknown",
                'Date': "Unknown",
                'Details': "Unknown",
                'Event Type': "Unknown",
                'Event Subtype': "Unknown",
                'URL': "Unknown"
            }
            
            # Заполняем данные из соответствующих столбцов
            for field, idx in column_indices.items():
                if idx is not None and idx < len(cols):
                    value = clean_text(cols[idx].text)
                    if field == 'death_toll':
                        if is_death_toll(value):
                            event_data['Death toll'] = value
                        elif is_date(value) and column_indices['date'] is None:
                            event_data['Date'] = format_date(value)
                    elif field == 'date':
                        if is_date(value) or is_year(value):
                            event_data['Date'] = format_date(value)
                        elif is_death_toll(value) and event_data['Death toll'] == "Unknown":
                            event_data['Death toll'] = value
                    elif field == 'location':
                        event_data['City'] = extract_city(value, event_data['Event'], event_data['Details'])
                        event_data['Country'] = determine_country(value, event_data['Event'], event_data['Details'])
                    elif field == 'event':
                        # Если название длинное, переносим его в Details
                        if len(value) > 50:
                            event_data['Details'] = value
                            event_data['Event'] = create_short_event_name(value, "")
                        else:
                            event_data['Event'] = value
                    elif field == 'details':
                        event_data['Details'] = value
            
            # Определяем тип и подтип события
            event_type, subtype = determine_event_type(event_data['Event'], event_data['Details'])
            event_data['Event Type'] = event_type
            event_data['Event Subtype'] = subtype
            
            # Проверяем корректность данных
            if event_data['Death toll'] != "Unknown" and not is_date(event_data['Death toll']):
                # Если страна не определена, пытаемся определить её из URL
                if event_data['Country'] == "Unknown":
                    event_data['Country'] = determine_country("", "", "", disaster_type)
                events.append(event_data)
    
    return events

def main():
    # URL главной страницы со списком катастроф
    main_url = "https://en.wikipedia.org/wiki/Lists_of_disasters"
    
    try:
        response = requests.get(main_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Находим все ссылки на списки катастроф
        disaster_links = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and 'List_of_' in href and 'disaster' in href.lower():
                full_url = f"https://en.wikipedia.org{href}"
                disaster_links.append(full_url)
        
        # Удаляем дубликаты
        disaster_links = list(set(disaster_links))
        
        all_events = []
        
        # Парсим каждую страницу
        for url in disaster_links:
            print(f"Парсинг страницы: {url}")
            events = parse_page(url)
            all_events.extend(events)
        
        # Создаем DataFrame
        df = pd.DataFrame(all_events)
        
        # Удаляем дубликаты и пустые строки
        df = df.drop_duplicates()
        df = df[df['Death toll'] != 'Unknown']
        
        # Удаляем колонку Type
        if 'Type' in df.columns:
            df = df.drop('Type', axis=1)
        
        # Сохраняем результаты в CSV
        df.to_csv('disasters.csv', index=False, encoding='utf-8')
        print("\nДанные успешно сохранены в файл 'disasters.csv'")
        
        # Выводим статистику
        print(f"\nВсего собрано событий: {len(df)}")
        print("\nРаспределение по основным типам катастроф:")
        print(df['Event Type'].value_counts())
        print("\nРаспределение по подтипам катастроф:")
        print(df['Event Subtype'].value_counts())
        print("\nРаспределение по странам:")
        print(df['Country'].value_counts())
        
        # Выводим результаты в консоль
        print("\nСписок катастроф:")
        print("-" * 80)
        for _, row in df.iterrows():
            print(f"Основной тип: {row['Event Type']}")
            print(f"Подтип: {row['Event Subtype']}")
            print(f"Страна: {row['Country']}")
            print(f"Город: {row['City']}")
            print(f"Смертей: {row['Death toll']}")
            print(f"Событие: {row['Event']}")
            print(f"Дата: {row['Date']}")
            if row['Details'] != "Unknown":
                print(f"Детали: {row['Details']}")
            print(f"URL: {row['URL']}")
            print("-" * 80)
            
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")

if __name__ == "__main__":
    main() 