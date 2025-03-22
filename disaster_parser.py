import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import concurrent.futures
import functools
from urllib.parse import urlparse
import os
import pickle
import time

# Кэш для запросов
request_cache = {}
CACHE_DIR = 'cache'
CACHE_EXPIRY = 3600  # 1 час

# Компилируем регулярные выражения один раз
YEAR_PATTERN = re.compile(r'\b(1\d{3}|20[0-2]\d)\b')
DATE_PATTERNS = [
    re.compile(r'\b(1\d{3}|20[0-2]\d)\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}\b'),
    re.compile(r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+(1\d{3}|20[0-2]\d)\b')
]
DEATH_PATTERNS = [
    re.compile(r'(\d+(?:,\d+)?)\s+deaths?'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+killed'),
    re.compile(r'(\d+(?:,\d+)?)\s+casualties'),
    re.compile(r'(\d+(?:,\d+)?)\s+fatalities'),
    re.compile(r'(\d+(?:,\d+)?)\s+dead'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+died'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+perished'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+lost\s+their\s+lives'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+were\s+killed'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+were\s+dead'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+were\s+found\s+dead'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+were\s+reported\s+dead'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+were\s+confirmed\s+dead'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+were\s+pronounced\s+dead'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+were\s+declared\s+dead'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+were\s+believed\s+to\s+have\s+died'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+were\s+thought\s+to\s+have\s+died'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+were\s+presumed\s+dead'),
    re.compile(r'(\d+(?:,\d+)?)\s+people\s+were\s+missing\s+and\s+presumed\s+dead')
]
NUMBERS_PATTERN = re.compile(r'\d+(?:,\d+)?')

def setup_cache():
    """Создает директорию для кэша если её нет"""
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)

def get_cached_response(url):
    """Получает ответ из кэша или делает новый запрос"""
    cache_file = os.path.join(CACHE_DIR, urlparse(url).path.replace('/', '_') + '.cache')
    
    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as f:
            cached_data = pickle.load(f)
            if time.time() - cached_data['timestamp'] < CACHE_EXPIRY:
                return cached_data['content']
    
    response = requests.get(url)
    content = response.content
    
    with open(cache_file, 'wb') as f:
        pickle.dump({
            'content': content,
            'timestamp': time.time()
        }, f)
    
    return content

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
    return bool(YEAR_PATTERN.search(str(text)))

def is_date(text):
    """Проверяет, является ли текст датой"""
    return any(pattern.search(str(text)) for pattern in DATE_PATTERNS)

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
        r'^\d+$',
        r'^\d{1,3}(,\d{3})*$',
        r'^\d{1,3}(,\d{3})*-\d{1,3}(,\d{3})*$',
        r'^\d+-\d+$',
        r'^[~c\. ]*\d+$',
        r'^[<>]\s*\d+$'
    ]
    
    for pattern in death_patterns:
        if re.match(pattern, text):
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
    
    # Определяем основной тип (nature, human_accident или human_deliberate)
    nature_keywords = ['earthquake', 'tsunami', 'flood', 'hurricane', 'tornado', 'storm', 
                      'volcano', 'avalanche', 'landslide', 'drought', 'famine', 'pandemic',
                      'epidemic', 'wildfire', 'cyclone', 'typhoon', 'blizzard', 'heat wave',
                      'cold wave', 'frost', 'hail', 'snow', 'rain', 'natural']
    
    deliberate_keywords = ['terrorist', 'terrorism', 'bombing', 'massacre', 'mass shooting',
                         'arson', 'sabotage', 'attack', 'assassination', 'genocide',
                         'war crime', 'ethnic cleansing', 'deliberate', 'intentional',
                         'planned', 'premeditated', 'murder']
    
    accident_keywords = ['explosion', 'fire', 'crash', 'collision', 'derailment', 'sinking',
                      'crush', 'stampede', 'industrial', 'mining', 'chemical', 'nuclear',
                      'radiation', 'leak', 'spill', 'accident', 'disaster', 'man-made']
    
    # Определяем основной тип
    event_type = 'human_accident'  # По умолчанию
    
    # Сначала проверяем на преднамеренные действия
    for keyword in deliberate_keywords:
        if keyword in event_text:
            event_type = 'human_deliberate'
            break
    
    # Если это не преднамеренное действие, проверяем природные причины
    if event_type == 'human_accident':
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
    
    # Преднамеренные действия человека
    elif event_type == 'human_deliberate':
        if any(word in event_text for word in ['terrorist', 'terrorism', 'bombing', 'bomb']):
            subtype = 'terrorism'
        elif any(word in event_text for word in ['shooting', 'massacre', 'mass killing']):
            subtype = 'mass_shooting'
        elif any(word in event_text for word in ['arson', 'deliberate fire']):
            subtype = 'arson'
        elif any(word in event_text for word in ['sabotage', 'vandalism']):
            subtype = 'sabotage'
        elif any(word in event_text for word in ['genocide', 'ethnic cleansing']):
            subtype = 'genocide'
    
    # Техногенные катастрофы (случайные)
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
    
    # Расширенный список ключевых слов
    keywords = [
        'disaster', 'accident', 'incident', 'explosion', 'fire', 'crash',
        'sinking', 'collision', 'derailment', 'flood', 'earthquake', 'tsunami',
        'storm', 'hurricane', 'tornado', 'avalanche', 'landslide', 'drought',
        'famine', 'pandemic', 'epidemic', 'wildfire', 'blizzard', 'heat wave',
        'cold wave', 'frost', 'hail', 'snow', 'rain', 'natural', 'industrial',
        'mining', 'chemical', 'nuclear', 'radiation', 'leak', 'spill', 'contamination',
        'environmental', 'pollution', 'toxic', 'poison', 'massacre', 'shooting',
        'terrorist', 'terrorism', 'bombing', 'bomb', 'mass killing'
    ]
    
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
            
            # Проверяем текст ссылки и детали на наличие ключевых слов
            text_to_check = f"{event_name} {details}".lower()
            if any(keyword in text_to_check for keyword in keywords):
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

def parse_environmental_disasters(soup):
    """Парсит страницу с экологическими катастрофами"""
    events = []
    
    # Находим все заголовки категорий (h2)
    categories = soup.find_all('h2')
    print(f"Найдено категорий: {len(categories)}")
    
    # Расширенный список ключевых слов для экологических катастроф
    env_keywords = {
        # Общие термины
        'oil spill', 'chemical spill', 'toxic waste', 'pollution', 'contamination',
        'environmental disaster', 'ecological disaster', 'environmental damage',
        'environmental impact', 'environmental catastrophe', 'environmental crisis',
        'environmental emergency', 'environmental incident', 'environmental accident',
        'environmental contamination', 'environmental pollution', 'environmental hazard',
        'environmental risk', 'environmental threat', 'environmental problem',
        'environmental issue', 'environmental concern', 'environmental damage',
        'environmental destruction', 'environmental degradation', 'environmental harm',
        'environmental injury', 'environmental impairment', 'environmental detriment',
        
        # Специфические типы загрязнений
        'water pollution', 'air pollution', 'soil pollution', 'groundwater pollution',
        'marine pollution', 'ocean pollution', 'river pollution', 'lake pollution',
        'industrial pollution', 'agricultural pollution', 'urban pollution',
        
        # Химические вещества
        'mercury', 'lead', 'arsenic', 'cadmium', 'chromium', 'pesticides',
        'herbicides', 'fertilizers', 'heavy metals', 'toxic chemicals',
        
        # Источники загрязнения
        'mining waste', 'industrial waste', 'nuclear waste', 'radioactive waste',
        'sewage', 'wastewater', 'effluent', 'emissions', 'discharge',
        
        # Последствия
        'ecosystem damage', 'biodiversity loss', 'habitat destruction',
        'species extinction', 'deforestation', 'desertification',
        'climate change', 'global warming', 'ozone depletion'
    }
    
    # Создаем множество для быстрого поиска
    env_keywords_set = set(env_keywords)
    
    for category in categories:
        category_name = category.text.strip()
        if not category_name or category_name in ['Contents', 'See also', 'References']:
            continue
            
        print(f"\nОбработка категории: {category_name}")
        
        # Получаем следующий ul после заголовка
        ul = category.find_next('ul')
        if not ul:
            print(f"Предупреждение: Не найден список для категории {category_name}")
            continue
            
        # Парсим все li элементы
        items = ul.find_all('li')
        print(f"Найдено элементов: {len(items)}")
        
        for li in items:
            text = clean_text_cached(li.text)
            if not text:
                continue
                
            # Ищем ссылку в li
            link = li.find('a')
            if not link:
                print(f"Предупреждение: Не найдена ссылка в элементе: {text[:50]}...")
                continue
                
            href = link.get('href')
            if not href or not href.startswith('/wiki/'):
                print(f"Предупреждение: Неверный формат ссылки: {href}")
                continue
                
            event_name = clean_text_cached(link.text)
            details = text.replace(event_name, '').strip()
            
            # Проверяем, является ли это экологической катастрофой
            text_to_check = f"{event_name} {details}".lower()
            if not any(keyword in text_to_check for keyword in env_keywords_set):
                print(f"Пропуск: Не найдены ключевые слова в тексте: {text[:50]}...")
                continue
            
            print(f"Обработка события: {event_name}")
            
            # Создаем данные события
            event_data = {
                'Death toll': "Unknown",
                'Event': create_short_event_name(event_name, details),
                'City': "Unknown",
                'Country': "Unknown",
                'Date': "Unknown",
                'Details': details,
                'Event Type': "human_accident",
                'Event Subtype': "environmental_disaster",
                'URL': f"https://en.wikipedia.org{href}"
            }
            
            # Извлекаем город и страну
            event_data['City'] = extract_city("", event_name, details)
            event_data['Country'] = determine_country("", event_name, details)
            
            # Ищем дату
            for pattern in DATE_PATTERNS:
                match = pattern.search(text)
                if match:
                    if len(match.group(0).split()) == 1:  # Только год
                        event_data['Date'] = f"{match.group(0)}-01-01"
                    else:  # Полная дата
                        event_data['Date'] = format_date(match.group(0))
                    print(f"Найдена дата: {event_data['Date']}")
                    break
            
            # Ищем количество жертв
            for pattern in DEATH_PATTERNS:
                match = pattern.search(text)
                if match:
                    event_data['Death toll'] = match.group(1)
                    print(f"Найдено количество жертв: {event_data['Death toll']}")
                    break
            
            # Если количество жертв не найдено, проверяем на наличие числовых данных
            if event_data['Death toll'] == "Unknown":
                numbers = NUMBERS_PATTERN.findall(text)
                for num in numbers:
                    if 1 <= int(num.replace(',', '')) <= 1000000:  # Разумный диапазон
                        event_data['Death toll'] = num
                        print(f"Найдено количество жертв из числовых данных: {event_data['Death toll']}")
                        break
            
            events.append(event_data)
            print(f"Событие добавлено: {event_data['Event']}")
    
    print(f"\nВсего извлечено событий: {len(events)}")
    return events

def parse_page_parallel(url):
    """Версия parse_page для параллельной обработки"""
    try:
        print(f"\nОбработка страницы: {url}")
        content = get_cached_response(url)
        soup = BeautifulSoup(content, 'html.parser')
        
        if 'List_of_environmental_disasters' in url:
            print("Обнаружена страница экологических катастроф, применяю специальный парсер")
            events = parse_environmental_disasters(soup)
            if not events:
                print("Предупреждение: Не удалось извлечь события со страницы экологических катастроф")
            return events
        
        tables = soup.find_all('table', {'class': 'wikitable'})
        all_events = []
        
        if tables:
            print(f"Найдено таблиц: {len(tables)}")
            disaster_type = clean_disaster_type(url)
            for table in tables:
                events = parse_table(table, disaster_type)
                for event in events:
                    event['URL'] = url
                    if event['Country'] == "Unknown":
                        country = extract_country_from_url(url)
                        if country:
                            event['Country'] = country
                all_events.extend(events)
        else:
            print("Таблицы не найдены, пытаюсь извлечь события из текста")
            events = extract_links_from_text(soup)
            for event in events:
                if event['Country'] == "Unknown":
                    country = extract_country_from_url(url)
                    if country:
                        event['Country'] = country
            all_events.extend(events)
        
        if not all_events:
            print("Предупреждение: Не удалось извлечь события со страницы")
        
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
    rows = table.find_all('tr')[1:]
    for row in rows:
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
                if event_data['Country'] == "Unknown":
                    event_data['Country'] = determine_country("", "", "", disaster_type)
                events.append(event_data)
    
    return events

def main():
    setup_cache()
    main_url = "https://en.wikipedia.org/wiki/Lists_of_disasters"
    
    try:
        content = get_cached_response(main_url)
        soup = BeautifulSoup(content, 'html.parser')
        
        # Собираем все ссылки
        disaster_links = []
        env_disaster_url = "https://en.wikipedia.org/wiki/List_of_environmental_disasters"
        
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and 'List_of_' in href and 'disaster' in href.lower():
                full_url = f"https://en.wikipedia.org{href}"
                if full_url != env_disaster_url:
                    disaster_links.append(full_url)
        
        disaster_links = list(set(disaster_links))
        
        # Добавляем URL экологических катастроф первым
        disaster_links.insert(0, env_disaster_url)
        
        # Параллельная обработка страниц
        all_events = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(parse_page_parallel, url): url for url in disaster_links}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    events = future.result()
                    all_events.extend(events)
                except Exception as e:
                    print(f"Ошибка при обработке {url}: {str(e)}")
        
        # Создаем DataFrame и сохраняем результаты
        df = pd.DataFrame(all_events)
        df = df.drop_duplicates()
        df = df[df['Death toll'] != 'Unknown']
        
        if 'Type' in df.columns:
            df = df.drop('Type', axis=1)
        
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
        print(f"Ошибка при парсинге: {str(e)}")

if __name__ == "__main__":
    main() 