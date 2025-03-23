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
import logging
from datetime import datetime
import argparse

# Очищаем лог-файл при запуске
if os.path.exists('parser.log'):
    with open('parser.log', 'w', encoding='utf-8') as f:
        f.write('')

# Настройка логирования
logging.basicConfig(
    filename='parser.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

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

def parse_args():
    """Парсит аргументы командной строки"""
    parser = argparse.ArgumentParser(description='Парсер катастроф из Wikipedia')
    parser.add_argument('--url', type=str, 
                      default='https://en.wikipedia.org/wiki/Lists_of_disasters',
                      help='URL страницы со списком катастроф')
    parser.add_argument('--output', type=str,
                      default='disasters.csv',
                      help='Путь к файлу для сохранения результатов')
    parser.add_argument('--workers', type=int,
                      default=10,
                      help='Количество параллельных потоков')
    return parser.parse_args()

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
    
    # Очищаем строку от лишних символов
    date_str = re.sub(r'[^\w\s\-/\.]', ' ', date_str)
    date_str = re.sub(r'\s+', ' ', date_str).strip()
    
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
        
        # DD/MM/YYYY или MM/DD/YYYY
        (r'(\d{1,2})/(\d{1,2})/(\d{4})',
         lambda m: f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"),
        
        # DD.MM.YYYY
        (r'(\d{1,2})\.(\d{1,2})\.(\d{4})',
         lambda m: f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"),
        
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
    
    # Проверка на валидность даты
    def is_valid_date(year, month, day):
        try:
            datetime(int(year), int(month), int(day))
            return True
        except ValueError:
            return False
    
    # Если нашли дату, проверяем её валидность
    for pattern, formatter in patterns:
        match = re.search(pattern, date_str)
        if match:
            date_str = formatter(match)
            year, month, day = date_str.split('-')
            if is_valid_date(year, month, day):
                return date_str
    
    # Если не нашли дату, пытаемся извлечь год
    year_match = re.search(r'\b(1\d{3}|20[0-2]\d)\b', date_str)
    if year_match:
        return f"{year_match.group(1)}-01-01"
    
    return "Unknown"

def extract_city(location, event_name, details):
    if not location and not event_name and not details:
        return "Unknown"
    
    text = f"{location} {event_name} {details}"
    
    # Очищаем текст от служебных слов и символов
    text = re.sub(r'\b(accidents?|and|disasters?|by|death|toll)\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'[^\w\s,]', ' ', text)
    
    # Список городов и их вариаций
    cities = {
        # Европа
        "London": ["London", "Blackfriars, London", "Greater London", "City of London", "Londinium"],
        "Paris": ["Paris", "Île-de-France", "Greater Paris", "Lutetia"],
        "Berlin": ["Berlin", "Greater Berlin", "Berlino"],
        "Madrid": ["Madrid", "Greater Madrid", "Villa de Madrid"],
        "Rome": ["Rome", "Greater Rome", "Roma", "Eternal City"],
        "Amsterdam": ["Amsterdam", "Greater Amsterdam", "Amsterdam City"],
        "Vienna": ["Vienna", "Greater Vienna", "Wien"],
        "Prague": ["Prague", "Greater Prague", "Praha"],
        "Moscow": ["Moscow", "Greater Moscow", "Moskva"],
        "Saint Petersburg": ["Saint Petersburg", "St. Petersburg", "Leningrad", "Petrograd"],
        
        # Северная Америка
        "New York": ["New York", "New York City", "NYC", "Manhattan", "Brooklyn", "Queens", "The Big Apple"],
        "Los Angeles": ["Los Angeles", "LA", "Greater Los Angeles", "City of Angels"],
        "Chicago": ["Chicago", "Greater Chicago", "Windy City"],
        "Toronto": ["Toronto", "Greater Toronto", "GTA", "The 6ix"],
        "Montreal": ["Montreal", "Greater Montreal", "Montréal"],
        "Vancouver": ["Vancouver", "Greater Vancouver", "VanCity"],
        "San Francisco": ["San Francisco", "SF", "Bay Area", "Frisco"],
        "Seattle": ["Seattle", "Greater Seattle", "Emerald City"],
        "Boston": ["Boston", "Greater Boston", "Beantown"],
        "Houston": ["Houston", "Greater Houston", "Space City"],
        
        # Азия
        "Tokyo": ["Tokyo", "Greater Tokyo", "Metropolitan Tokyo", "Tōkyō", "Edo"],
        "Beijing": ["Beijing", "Peking", "Greater Beijing", "Běijīng"],
        "Shanghai": ["Shanghai", "Greater Shanghai", "Shànghǎi"],
        "Hong Kong": ["Hong Kong", "HK", "Special Administrative Region", "Xiānggǎng"],
        "Singapore": ["Singapore", "SG", "Republic of Singapore", "Singapura"],
        "Seoul": ["Seoul", "Greater Seoul", "Sŏul"],
        "Bangkok": ["Bangkok", "Greater Bangkok", "Krung Thep"],
        "Mumbai": ["Mumbai", "Bombay", "Greater Mumbai", "Mumbaī"],
        "Delhi": ["Delhi", "New Delhi", "Greater Delhi", "Dilli"],
        "Dubai": ["Dubai", "Greater Dubai", "Dubayy"],
        
        # Австралия и Океания
        "Sydney": ["Sydney", "Greater Sydney", "Harbour City"],
        "Melbourne": ["Melbourne", "Greater Melbourne", "Melbin"],
        "Brisbane": ["Brisbane", "Greater Brisbane", "Brisvegas"],
        "Auckland": ["Auckland", "Greater Auckland", "Tāmaki Makaurau"],
        "Wellington": ["Wellington", "Greater Wellington", "Te Whanganui-a-Tara"],
        
        # Южная Америка
        "São Paulo": ["São Paulo", "Sao Paulo", "Greater São Paulo", "Sampa"],
        "Rio de Janeiro": ["Rio de Janeiro", "Rio", "Greater Rio", "Cidade Maravilhosa"],
        "Buenos Aires": ["Buenos Aires", "Greater Buenos Aires", "Ciudad Autónoma"],
        "Santiago": ["Santiago", "Greater Santiago", "Santiago de Chile"],
        "Lima": ["Lima", "Greater Lima", "Ciudad de los Reyes"],
        
        # Африка
        "Cairo": ["Cairo", "Greater Cairo", "Al-Qāhirah"],
        "Johannesburg": ["Johannesburg", "Joburg", "Greater Johannesburg", "Jozi"],
        "Cape Town": ["Cape Town", "Greater Cape Town", "Kaapstad"],
        "Nairobi": ["Nairobi", "Greater Nairobi", "Green City in the Sun"],
        "Lagos": ["Lagos", "Greater Lagos", "Eko"]
    }
    
    # Поиск города в тексте
    for city, variations in cities.items():
        for variation in variations:
            if variation.lower() in text.lower():
                return city
    
    # Извлечение города из текста с помощью регулярных выражений
    city_patterns = [
        r'in ([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?:,|\s|$)',
        r'at ([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?:,|\s|$)',
        r'near ([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?:,|\s|$)',
        r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?:,|\s|$)',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+disaster',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+accident',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+incident',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+city',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+town',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+port'
    ]
    
    for pattern in city_patterns:
        match = re.search(pattern, text)
        if match:
            city = match.group(1)
            # Проверяем, что это не страна или регион
            if city not in ["England", "Scotland", "Wales", "Ireland", "Britain", "UK",
                          "France", "Germany", "Italy", "Spain", "Russia", "China",
                          "Japan", "India", "Brazil", "Canada", "Australia", "New Zealand",
                          "South Africa", "Egypt", "Turkey", "Greece", "Poland", "Czech",
                          "Hungary", "Romania", "Bulgaria", "Israel", "Mexico", "South Korea",
                          "Netherlands", "Switzerland", "Sweden", "Norway", "Denmark", "Finland",
                          "United States", "United Kingdom", "United Arab Emirates", "Saudi Arabia",
                          "South America", "North America", "Europe", "Asia", "Africa", "Oceania"]:
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
    
    # Очищаем текст от служебных слов и символов
    text = re.sub(r'\b(accidents?|and|disasters?|by|death|toll)\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'[^\w\s,]', ' ', text)
    
    # Расширенный словарь стран и их вариаций
    countries = {
        "United Kingdom": [
            "UK", "U.K.", "Britain", "Great Britain", "England", "Scotland", "Wales",
            "Northern Ireland", "Glasgow, Scotland", "Edinburgh, Scotland",
            "London, England", "Cardiff, Wales", "Belfast, Northern Ireland",
            "County Durham", "Yorkshire", "Lancashire", "Cornwall", "Devon",
            "Glamorganshire", "Monmouthshire", "Staffordshire", "Derbyshire",
            "Isle of Man", "Isles of Scilly", "Shetland", "Thames", "British",
            "United Kingdom of Great Britain and Northern Ireland"
        ],
        "Ireland": [
            "Ireland", "Dublin", "County Galway", "County Donegal", "Bantry Bay",
            "Republic of Ireland", "Irish", "Eire"
        ],
        "France": [
            "France", "Beauvais", "Brittany", "Aarsele", "French", "Paris",
            "Normandy", "Bordeaux", "Lyon", "Marseille"
        ],
        "United States": [
            "USA", "U.S.", "U.S.A.", "United States", "America", "American",
            "New York", "California", "Texas", "Florida", "Washington DC",
            "Chicago", "Los Angeles", "Houston", "Philadelphia"
        ],
        "Australia": [
            "Australia", "Victoria", "Tasmania", "Australian", "Sydney",
            "Melbourne", "Brisbane", "Perth", "Adelaide", "Canberra"
        ],
        "Austria": [
            "Austria", "Innsbruck", "Austrian", "Vienna", "Salzburg",
            "Graz", "Linz", "Tyrol", "Styria"
        ],
        "Belgium": [
            "Belgium", "Aarsele", "Belgian", "Brussels", "Antwerp",
            "Ghent", "Bruges", "Wallonia", "Flanders"
        ],
        "Germany": [
            "Germany", "Bavaria", "German", "Berlin", "Munich",
            "Hamburg", "Frankfurt", "Cologne", "Dresden"
        ],
        "Japan": [
            "Japan", "Japanese", "Tokyo", "Osaka", "Kyoto",
            "Yokohama", "Nagoya", "Hiroshima", "Nagasaki"
        ],
        "China": [
            "China", "Chinese", "Beijing", "Shanghai", "Hong Kong",
            "Guangzhou", "Shenzhen", "Chengdu", "Xi'an"
        ],
        "Russia": [
            "Russia", "Russian", "Moscow", "Saint Petersburg", "Novosibirsk",
            "Yekaterinburg", "Kazan", "Nizhny Novgorod", "Siberia"
        ],
        "India": [
            "India", "Indian", "Mumbai", "Delhi", "Bangalore",
            "Chennai", "Kolkata", "Hyderabad", "Pune"
        ],
        "Brazil": [
            "Brazil", "Brazilian", "São Paulo", "Rio de Janeiro", "Brasília",
            "Salvador", "Fortaleza", "Belo Horizonte", "Manaus"
        ],
        "Canada": [
            "Canada", "Canadian", "Toronto", "Montreal", "Vancouver",
            "Calgary", "Edmonton", "Ottawa", "Quebec"
        ],
        "Italy": [
            "Italy", "Italian", "Rome", "Milan", "Venice",
            "Florence", "Naples", "Turin", "Bologna"
        ],
        "Spain": [
            "Spain", "Spanish", "Madrid", "Barcelona", "Valencia",
            "Seville", "Bilbao", "Granada", "Malaga"
        ],
        "Mexico": [
            "Mexico", "Mexican", "Mexico City", "Guadalajara", "Monterrey",
            "Puebla", "Tijuana", "Cancun", "Acapulco"
        ],
        "South Korea": [
            "South Korea", "Korean", "Seoul", "Busan", "Incheon",
            "Daegu", "Daejeon", "Gwangju", "Suwon"
        ],
        "Netherlands": [
            "Netherlands", "Dutch", "Amsterdam", "Rotterdam", "The Hague",
            "Utrecht", "Eindhoven", "Groningen", "Haarlem"
        ],
        "Switzerland": [
            "Switzerland", "Swiss", "Zurich", "Geneva", "Basel",
            "Bern", "Lausanne", "Lugano", "St. Moritz"
        ],
        "Sweden": [
            "Sweden", "Swedish", "Stockholm", "Gothenburg", "Malmö",
            "Uppsala", "Västerås", "Örebro", "Linköping"
        ],
        "Norway": [
            "Norway", "Norwegian", "Oslo", "Bergen", "Trondheim",
            "Stavanger", "Drammen", "Fredrikstad", "Kristiansand"
        ],
        "Denmark": [
            "Denmark", "Danish", "Copenhagen", "Aarhus", "Odense",
            "Aalborg", "Frederiksberg", "Esbjerg", "Gentofte"
        ],
        "Finland": [
            "Finland", "Finnish", "Helsinki", "Espoo", "Tampere",
            "Vantaa", "Oulu", "Turku", "Jyväskylä"
        ],
        "Poland": [
            "Poland", "Polish", "Warsaw", "Kraków", "Łódź",
            "Wrocław", "Poznań", "Gdańsk", "Szczecin"
        ],
        "Czech Republic": [
            "Czech Republic", "Czech", "Prague", "Brno", "Ostrava",
            "Plzeň", "Liberec", "Olomouc", "České Budějovice"
        ],
        "Hungary": [
            "Hungary", "Hungarian", "Budapest", "Debrecen", "Szeged",
            "Miskolc", "Pécs", "Győr", "Nyíregyháza"
        ],
        "Romania": [
            "Romania", "Romanian", "Bucharest", "Cluj-Napoca", "Timișoara",
            "Iași", "Constanța", "Craiova", "Galați"
        ],
        "Bulgaria": [
            "Bulgaria", "Bulgarian", "Sofia", "Plovdiv", "Varna",
            "Burgas", "Ruse", "Stara Zagora", "Sliven"
        ],
        "Greece": [
            "Greece", "Greek", "Athens", "Thessaloniki", "Patras",
            "Heraklion", "Larissa", "Volos", "Rhodes"
        ],
        "Turkey": [
            "Turkey", "Turkish", "Istanbul", "Ankara", "İzmir",
            "Bursa", "Antalya", "Adana", "Gaziantep"
        ],
        "Israel": [
            "Israel", "Israeli", "Jerusalem", "Tel Aviv", "Haifa",
            "Rishon LeZion", "Petah Tikva", "Ashdod", "Netanya"
        ],
        "Egypt": [
            "Egypt", "Egyptian", "Cairo", "Alexandria", "Giza",
            "Shubra El Kheima", "Port Said", "Suez", "Luxor"
        ],
        "South Africa": [
            "South Africa", "South African", "Johannesburg", "Cape Town",
            "Durban", "Pretoria", "Port Elizabeth", "Bloemfontein", "Kimberley"
        ],
        "New Zealand": [
            "New Zealand", "New Zealander", "Auckland", "Wellington",
            "Christchurch", "Hamilton", "Tauranga", "Napier-Hastings", "Dunedin"
        ]
    }
    
    # Поиск страны в тексте
    for country, variations in countries.items():
        for variation in variations:
            if variation.lower() in text.lower():
                return country
    
    # Если страна не найдена в тексте, пытаемся извлечь из URL
    if url:
        country_from_url = extract_country_from_url(url)
        if country_from_url:
            return country_from_url
    
    # Пытаемся извлечь страну из текста с помощью регулярных выражений
    country_patterns = [
        r'in ([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?:,|\s|$)',
        r'at ([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?:,|\s|$)',
        r'near ([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?:,|\s|$)',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+disaster',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+accident',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+incident'
    ]
    
    for pattern in country_patterns:
        match = re.search(pattern, text)
        if match:
            potential_country = match.group(1)
            # Проверяем, что это действительно страна
            if potential_country in [country for variations in countries.values() for country in variations]:
                return potential_country
    
    return "Unknown"

def create_short_event_name(event_name, details):
    """Создает краткое название события на основе полного описания"""
    if not event_name or event_name == "Unknown":
        return "Unknown"
    
    # Если название уже короткое (менее 50 символов), оставляем его как есть
    if len(event_name) < 50:
        return event_name
    
    # Ищем ключевые слова в названии
    keywords = ['disaster', 'accident', 'incident', 'tragedy', 'catastrophe', 'crisis',
                'emergency', 'outbreak', 'epidemic', 'pandemic', 'plague',
                
                # Природные катастрофы
                'earthquake', 'tsunami', 'flood', 'hurricane', 'tornado', 'storm',
                'volcano', 'avalanche', 'landslide', 'drought', 'famine',
                'wildfire', 'blizzard', 'heat wave', 'cold wave', 'frost',
                'hail', 'snow', 'rain', 'natural', 'cyclone', 'typhoon',
                
                # Техногенные катастрофы
                'explosion', 'fire', 'crash', 'collision', 'derailment', 'sinking',
                'crush', 'stampede', 'industrial', 'mining', 'chemical',
                'nuclear', 'radiation', 'leak', 'spill', 'contamination',
                'environmental', 'pollution', 'toxic', 'poison',
                
                # Терроризм и насилие
                'massacre', 'shooting', 'terrorist', 'terrorism', 'bombing',
                'bomb', 'mass killing', 'attack', 'assassination', 'genocide',
                'ethnic cleansing', 'mass murder', 'mass death',
                
                # Болезни и эпидемии
                'disease', 'virus', 'infection', 'outbreak', 'epidemic',
                'pandemic', 'plague', 'contagion', 'virus outbreak',
                'disease outbreak', 'health crisis', 'medical emergency']
    
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
        # Общие термины
        'disaster', 'accident', 'incident', 'tragedy', 'catastrophe', 'crisis',
        'emergency', 'outbreak', 'epidemic', 'pandemic', 'plague',
        
        # Природные катастрофы
        'earthquake', 'tsunami', 'flood', 'hurricane', 'tornado', 'storm',
        'volcano', 'avalanche', 'landslide', 'drought', 'famine',
        'wildfire', 'blizzard', 'heat wave', 'cold wave', 'frost',
        'hail', 'snow', 'rain', 'natural', 'cyclone', 'typhoon',
        
        # Техногенные катастрофы
        'explosion', 'fire', 'crash', 'collision', 'derailment', 'sinking',
        'crush', 'stampede', 'industrial', 'mining', 'chemical',
        'nuclear', 'radiation', 'leak', 'spill', 'contamination',
        'environmental', 'pollution', 'toxic', 'poison',
        
        # Терроризм и насилие
        'massacre', 'shooting', 'terrorist', 'terrorism', 'bombing',
        'bomb', 'mass killing', 'attack', 'assassination', 'genocide',
        'ethnic cleansing', 'mass murder', 'mass death',
        
        # Болезни и эпидемии
        'disease', 'virus', 'infection', 'outbreak', 'epidemic',
        'pandemic', 'plague', 'contagion', 'virus outbreak',
        'disease outbreak', 'health crisis', 'medical emergency',
        
        # Животные и насекомые
        'bees', 'wasps', 'snakes', 'spiders', 'sharks', 'bears',
        'lions', 'tigers', 'wolves', 'crocodiles', 'alligators',
        'venomous', 'poisonous', 'deadly', 'killer',
        
        # Транспортные катастрофы
        'air crash', 'plane crash', 'aircraft accident', 'train crash',
        'train derailment', 'shipwreck', 'ship sinking', 'boat accident',
        'bus crash', 'car accident', 'traffic accident', 'collision',
        
        # Промышленные катастрофы
        'factory accident', 'industrial accident', 'mining accident',
        'chemical plant', 'power plant', 'refinery', 'nuclear plant',
        'dam failure', 'bridge collapse', 'building collapse',
        
        # Экологические катастрофы
        'oil spill', 'chemical spill', 'toxic waste', 'hazardous waste',
        'environmental disaster', 'ecological catastrophe', 'pollution',
        'contamination', 'deforestation', 'desertification',
        
        # Стихийные бедствия
        'natural disaster', 'act of god', 'force majeure',
        'extreme weather', 'severe weather', 'climate disaster',
        'weather disaster', 'meteorological disaster'
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
            
            # Проверяем наличие ключевых слов
            has_keywords = any(keyword.lower() in text_to_check for keyword in keywords)
            
            # Проверяем наличие упоминаний о смертях или жертвах
            has_deaths = any(pattern.search(text_to_check) for pattern in DEATH_PATTERNS)
            
            # Проверяем наличие даты
            has_date = any(pattern.search(text_to_check) for pattern in DATE_PATTERNS) or is_year(text_to_check)
            
            # Если есть ключевые слова или упоминания о смертях, или дата
            if has_keywords or has_deaths or has_date:
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
                
                # Пытаемся извлечь дату
                for pattern in DATE_PATTERNS:
                    match = pattern.search(text_to_check)
                    if match:
                        event_data['Date'] = format_date(match.group(0))
                        break
                
                # Если дата не найдена, пытаемся найти год
                if event_data['Date'] == "Unknown":
                    year_match = YEAR_PATTERN.search(text_to_check)
                    if year_match:
                        event_data['Date'] = format_date(year_match.group(0))
                
                # Пытаемся извлечь количество жертв
                for pattern in DEATH_PATTERNS:
                    match = pattern.search(text_to_check)
                    if match:
                        event_data['Death toll'] = process_death_toll(match.group(1))
                        break
                
                events.append(event_data)
                logging.info(f"Успешно обработано событие из текста: {event_data['Event']} ({event_data['Date']}) в {event_data['City']}, {event_data['Country']}. "
                            f"Тип: {event_data['Event Type']}, Подтип: {event_data['Event Subtype']}, "
                            f"Жертв: {event_data['Death toll']}")
    
    return events

def parse_lists(soup):
    """Парсинг маркированных списков на странице экологических катастроф"""
    events = []
    
    # Находим все списки
    lists = soup.find_all(['ul', 'ol'])
    
    # Расширенный список ключевых слов для экологических катастроф
    env_keywords = {
        # Общие термины
        'pollution', 'contamination', 'spill', 'leak', 'discharge', 'emission',
        'environmental disaster', 'ecological catastrophe', 'environmental impact',
        'environmental damage', 'environmental crisis', 'environmental emergency',
        
        # Типы загрязнений
        'oil spill', 'chemical spill', 'toxic spill', 'hazardous waste',
        'radioactive contamination', 'nuclear accident', 'radiation leak',
        'air pollution', 'water pollution', 'soil contamination',
        'groundwater contamination', 'marine pollution', 'ocean pollution',
        
        # Химические вещества
        'mercury', 'arsenic', 'lead', 'cadmium', 'dioxin', 'PCB',
        'pesticide', 'herbicide', 'insecticide', 'chemical waste',
        'industrial waste', 'toxic waste', 'hazardous material',
        
        # Источники загрязнения
        'industrial accident', 'mining accident', 'factory accident',
        'power plant accident', 'refinery accident', 'chemical plant',
        'waste disposal', 'landfill', 'incinerator', 'sewage',
        
        # Последствия
        'ecosystem damage', 'biodiversity loss', 'species extinction',
        'habitat destruction', 'environmental degradation',
        'climate change impact', 'global warming effect',
        
        # Экологические термины
        'eutrophication', 'acid rain', 'ozone depletion',
        'deforestation', 'desertification', 'soil erosion',
        'water scarcity', 'air quality', 'environmental health'
    }
    
    # Преобразуем список в множество для быстрого поиска
    env_keywords_set = set(env_keywords)
    
    for list_elem in lists:
        # Пропускаем списки в навигации и других служебных элементах
        if list_elem.find_parent(['nav', 'div', 'table']) and not list_elem.find_parent('div', class_='mw-parser-output'):
            continue
            
        # Получаем заголовок раздела
        section_title = ""
        prev_elem = list_elem.find_previous(['h2', 'h3', 'h4'])
        if prev_elem:
            section_title = prev_elem.get_text().strip()
        
        # Обрабатываем элементы списка
        for item in list_elem.find_all('li'):
            text = item.get_text().strip()
            if not text:
                continue
                
            # Проверяем наличие ключевых слов
            text_lower = text.lower()
            if not any(keyword in text_lower for keyword in env_keywords_set):
                continue
                
            # Создаем событие
            event = {
                'type': 'environmental',
                'subtype': 'pollution',
                'country': 'Unknown',
                'city': 'Unknown',
                'deaths': 0,
                'date': None,
                'name': text,
                'url': None,
                'details': '',
                'section': section_title
            }
            
            # Ищем ссылку в элементе списка
            link = item.find('a')
            if link and link.get('href'):
                event['url'] = f"https://en.wikipedia.org{link['href']}"
            
            # Пытаемся извлечь город и страну из названия
            if ',' in text:
                parts = text.split(',')
                event['city'] = parts[0].strip()
                event['country'] = parts[-1].strip()
            
            # Пытаемся найти дату в названии
            for pattern in DATE_PATTERNS:
                match = pattern.search(text)
                if match:
                    event['date'] = match.group(0)
                    break
            
            # Пытаемся найти количество жертв
            for pattern in DEATH_PATTERNS:
                match = pattern.search(text)
                if match:
                    event['deaths'] = int(match.group(1))
                    break
            
            # Проверяем, что событие содержит количество жертв
            if event['deaths'] > 0:
                events.append(event)
                logging.info(f"Успешно обработана экологическая катастрофа из списка: {text}")
    
    return events

def parse_environmental_disasters(soup):
    """Парсинг страницы экологических катастроф"""
    events = []
    
    # Расширенный список ключевых слов для экологических катастроф
    env_keywords = {
        # Общие термины
        'pollution', 'contamination', 'spill', 'leak', 'discharge', 'emission',
        'environmental disaster', 'ecological catastrophe', 'environmental impact',
        'environmental damage', 'environmental crisis', 'environmental emergency',
        
        # Типы загрязнений
        'oil spill', 'chemical spill', 'toxic spill', 'hazardous waste',
        'radioactive contamination', 'nuclear accident', 'radiation leak',
        'air pollution', 'water pollution', 'soil contamination',
        'groundwater contamination', 'marine pollution', 'ocean pollution',
        
        # Химические вещества
        'mercury', 'arsenic', 'lead', 'cadmium', 'dioxin', 'PCB',
        'pesticide', 'herbicide', 'insecticide', 'chemical waste',
        'industrial waste', 'toxic waste', 'hazardous material',
        
        # Источники загрязнения
        'industrial accident', 'mining accident', 'factory accident',
        'power plant accident', 'refinery accident', 'chemical plant',
        'waste disposal', 'landfill', 'incinerator', 'sewage',
        
        # Последствия
        'ecosystem damage', 'biodiversity loss', 'species extinction',
        'habitat destruction', 'environmental degradation',
        'climate change impact', 'global warming effect',
        
        # Экологические термины
        'eutrophication', 'acid rain', 'ozone depletion',
        'deforestation', 'desertification', 'soil erosion',
        'water scarcity', 'air quality', 'environmental health'
    }
    
    # Преобразуем список в множество для быстрого поиска
    env_keywords_set = set(env_keywords)
    
    # 1. Парсинг таблиц
    tables = soup.find_all('table', class_='wikitable')
    for table in tables:
        rows = table.find_all('tr')
        if len(rows) < 2:  # Пропускаем пустые таблицы
            continue
            
        # Определяем заголовки
        headers = [th.get_text().strip().lower() for th in rows[0].find_all(['th', 'td'])]
        
        # Определяем индексы нужных столбцов
        date_idx = next((i for i, h in enumerate(headers) if any(word in h for word in ['date', 'year', 'when'])), None)
        event_idx = next((i for i, h in enumerate(headers) if any(word in h for word in ['event', 'disaster', 'incident'])), None)
        location_idx = next((i for i, h in enumerate(headers) if any(word in h for word in ['location', 'place', 'where', 'country'])), None)
        details_idx = next((i for i, h in enumerate(headers) if any(word in h for word in ['details', 'description', 'notes'])), None)
        
        # Обрабатываем строки
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) < max(filter(None, [date_idx, event_idx, location_idx, details_idx])):
                continue
                
            event = {
                'type': 'environmental',
                'subtype': 'pollution',
                'country': 'Unknown',
                'city': 'Unknown',
                'deaths': 0,
                'date': None,
                'name': '',
                'url': None,
                'details': ''
            }
            
            # Извлекаем данные из ячеек
            if date_idx is not None and date_idx < len(cells):
                event['date'] = cells[date_idx].get_text().strip()
            
            if event_idx is not None and event_idx < len(cells):
                event['name'] = cells[event_idx].get_text().strip()
                # Ищем ссылку в ячейке события
                link = cells[event_idx].find('a')
                if link and link.get('href'):
                    event['url'] = f"https://en.wikipedia.org{link['href']}"
            
            if location_idx is not None and location_idx < len(cells):
                location = cells[location_idx].get_text().strip()
                if ',' in location:
                    parts = location.split(',')
                    event['city'] = parts[0].strip()
                    event['country'] = parts[-1].strip()
                else:
                    event['country'] = location
            
            if details_idx is not None and details_idx < len(cells):
                event['details'] = cells[details_idx].get_text().strip()
            
            # Проверяем наличие ключевых слов в названии или деталях
            text_to_check = f"{event['name']} {event['details']}".lower()
            if any(keyword in text_to_check for keyword in env_keywords_set):
                events.append(event)
                logging.info(f"Успешно обработана экологическая катастрофа из таблицы: {event['name']}")
    
    # 2. Парсинг списков
    list_events = parse_lists(soup)
    events.extend(list_events)
    
    # 3. Парсинг категорий
    categories = soup.find_all('div', class_='mw-category')
    for category in categories:
        # Пропускаем нерелевантные категории
        if 'environmental' not in category.get_text().lower() and 'pollution' not in category.get_text().lower():
            continue
            
        # Ищем все ссылки в категории
        links = category.find_all('a')
        for link in links:
            if not link.get('href'):
                continue
                
            # Проверяем, что это ссылка на статью
            if not link['href'].startswith('/wiki/'):
                continue
                
            # Пропускаем служебные страницы
            if any(x in link['href'].lower() for x in ['template:', 'category:', 'file:', 'help:', 'portal:']):
                continue
                
            # Получаем текст ссылки
            text = link.get_text().strip()
            if not text:
                continue
                
            # Проверяем наличие ключевых слов
            text_lower = text.lower()
            if not any(keyword in text_lower for keyword in env_keywords_set):
                continue
                
            # Создаем событие
            event = {
                'type': 'environmental',
                'subtype': 'pollution',
                'country': 'Unknown',
                'city': 'Unknown',
                'deaths': 0,
                'date': None,
                'name': text,
                'url': f"https://en.wikipedia.org{link['href']}",
                'details': ''
            }
            
            # Пытаемся извлечь город и страну из названия
            if ',' in text:
                parts = text.split(',')
                event['city'] = parts[0].strip()
                event['country'] = parts[-1].strip()
            
            # Пытаемся найти дату в названии
            for pattern in DATE_PATTERNS:
                match = pattern.search(text)
                if match:
                    event['date'] = match.group(0)
                    break
            
            # Пытаемся найти количество жертв
            for pattern in DEATH_PATTERNS:
                match = pattern.search(text)
                if match:
                    event['deaths'] = int(match.group(1))
                    break
            
            events.append(event)
            logging.info(f"Успешно обработана экологическая катастрофа из категории: {text}")
    
    return events

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
                'Death toll': 0,
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
                            event_data['Death toll'] = process_death_toll(value)
                        elif is_date(value) and column_indices['date'] is None:
                            event_data['Date'] = format_date(value)
                    elif field == 'date':
                        if is_date(value) or is_year(value):
                            event_data['Date'] = format_date(value)
                        elif is_death_toll(value) and event_data['Death toll'] == 0:
                            event_data['Death toll'] = process_death_toll(value)
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
            if event_data['Country'] == "Unknown":
                event_data['Country'] = determine_country("", "", "", disaster_type)
            
            # Логируем успешно обработанное событие
            logging.info(f"Успешно обработано событие: {event_data['Event']} ({event_data['Date']}) в {event_data['City']}, {event_data['Country']}. "
                        f"Тип: {event_data['Event Type']}, Подтип: {event_data['Event Subtype']}, "
                        f"Жертв: {event_data['Death toll']}")
            
            events.append(event_data)
    
    return events

def parse_li_events(li_element, url):
    """Парсит событие из элемента LI и при необходимости переходит на второй уровень"""
    event_data = {
        'Death toll': "Unknown",
        'Event': "Unknown",
        'City': "Unknown",
        'Country': "Unknown",
        'Date': "Unknown",
        'Details': "Unknown",
        'Event Type': "Unknown",
        'Event Subtype': "Unknown",
        'URL': url
    }
    
    # Получаем текст элемента
    text = li_element.get_text().strip()
    if not text:
        return None
    
    # Ищем ссылку в элементе
    link = li_element.find('a')
    if link and link.get('href'):
        event_data['URL'] = f"https://en.wikipedia.org{link['href']}"
        event_data['Event'] = link.get_text().strip()
    
    # Проверяем наличие необходимой информации
    has_date = any(pattern.search(text) for pattern in DATE_PATTERNS) or is_year(text)
    has_deaths = any(pattern.search(text) for pattern in DEATH_PATTERNS)
    has_location = bool(re.search(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*', text))
    
    # Если информации недостаточно и есть ссылка, переходим на второй уровень
    if (not has_date or not has_deaths or not has_location) and link and link.get('href'):
        try:
            sub_url = f"https://en.wikipedia.org{link['href']}"
            content = get_cached_response(sub_url)
            sub_soup = BeautifulSoup(content, 'html.parser')
            
            # Ищем информацию в основном контенте
            main_content = sub_soup.find('div', class_='mw-parser-output')
            if main_content:
                # Ищем дату
                if not has_date:
                    for pattern in DATE_PATTERNS:
                        match = pattern.search(main_content.get_text())
                        if match:
                            event_data['Date'] = format_date(match.group(0))
                            break
                    if event_data['Date'] == "Unknown":
                        year_match = YEAR_PATTERN.search(main_content.get_text())
                        if year_match:
                            event_data['Date'] = format_date(year_match.group(0))
                
                # Ищем количество жертв
                if not has_deaths:
                    for pattern in DEATH_PATTERNS:
                        match = pattern.search(main_content.get_text())
                        if match:
                            event_data['Death toll'] = process_death_toll(match.group(1))
                            break
                
                # Ищем местоположение
                if not has_location:
                    event_data['City'] = extract_city("", event_data['Event'], main_content.get_text())
                    event_data['Country'] = determine_country("", event_data['Event'], main_content.get_text())
        except Exception as e:
            logging.error(f"Ошибка при парсинге второго уровня {sub_url}: {str(e)}")
    
    # Если все еще нет информации, пытаемся извлечь из текущего текста
    if event_data['Date'] == "Unknown":
        for pattern in DATE_PATTERNS:
            match = pattern.search(text)
            if match:
                event_data['Date'] = format_date(match.group(0))
                break
        if event_data['Date'] == "Unknown":
            year_match = YEAR_PATTERN.search(text)
            if year_match:
                event_data['Date'] = format_date(year_match.group(0))
    
    if event_data['Death toll'] == "Unknown":
        for pattern in DEATH_PATTERNS:
            match = pattern.search(text)
            if match:
                event_data['Death toll'] = process_death_toll(match.group(1))
                break
    
    if event_data['City'] == "Unknown" or event_data['Country'] == "Unknown":
        event_data['City'] = extract_city("", event_data['Event'], text)
        event_data['Country'] = determine_country("", event_data['Event'], text)
    
    # Определяем тип и подтип события
    event_type, subtype = determine_event_type(event_data['Event'], text)
    event_data['Event Type'] = event_type
    event_data['Event Subtype'] = subtype
    
    # Добавляем детали
    event_data['Details'] = text
    
    return event_data

def parse_page_parallel(url):
    """Версия parse_page для параллельной обработки"""
    try:
        print(f"\nОбработка страницы: {url}")
        content = get_cached_response(url)
        soup = BeautifulSoup(content, 'html.parser')
        
        all_events = []
        
        # Ищем все элементы LI в основном контенте
        main_content = soup.find('div', class_='mw-parser-output')
        if main_content:
            li_elements = main_content.find_all('li')
            for li in li_elements:
                event = parse_li_events(li, url)
                if event:
                    all_events.append(event)
                    logging.info(f"Успешно обработано событие: {event['Event']} ({event['Date']}) в {event['City']}, {event['Country']}. "
                                f"Тип: {event['Event Type']}, Подтип: {event['Event Subtype']}, "
                                f"Жертв: {event['Death toll']}")
        
        if not all_events:
            print("Предупреждение: Не удалось извлечь события со страницы")
        
        return all_events
    
    except Exception as e:
        print(f"Ошибка при парсинге {url}: {str(e)}")
        logging.error(f"Ошибка при парсинге {url}: {str(e)}")
        return []

def process_death_toll(death_toll_str):
    """Обрабатывает строку с количеством жертв"""
    if not death_toll_str or death_toll_str == "Unknown":
        return 0
    
    # Убираем запятые и кавычки
    death_toll_str = death_toll_str.replace(',', '').replace('"', '').replace("'", "")
    
    # Обработка диапазона чисел
    if '-' in death_toll_str:
        numbers = [int(n) for n in re.findall(r'\d+', death_toll_str)]
        if numbers:
            return sum(numbers) // len(numbers)
    
    # Обработка одиночного числа
    numbers = re.findall(r'\d+', death_toll_str)
    if numbers:
        return int(numbers[0])
    
    return 0

def main():
    # Парсим аргументы командной строки
    args = parse_args()
    
    # Создаем директорию для логов, если её нет
    os.makedirs('logs', exist_ok=True)
    
    # Логируем начало работы парсера
    logging.info("Начало работы парсера катастроф")
    logging.info(f"URL: {args.url}")
    logging.info(f"Выходной файл: {args.output}")
    logging.info(f"Количество рабочих потоков: {args.workers}")
    
    setup_cache()
    main_url = args.url
    
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
        
        logging.info(f"Найдено {len(disaster_links)} ссылок на страницы катастроф")
        
        # Параллельная обработка страниц
        all_events = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_url = {executor.submit(parse_page_parallel, url): url for url in disaster_links}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    events = future.result()
                    all_events.extend(events)
                except Exception as e:
                    print(f"Ошибка при обработке {url}: {str(e)}")
                    logging.error(f"Ошибка при обработке {url}: {str(e)}")
        
        # Создаем DataFrame и сохраняем результаты
        df = pd.DataFrame(all_events)
        df = df.drop_duplicates()
        
        if 'Type' in df.columns:
            df = df.drop('Type', axis=1)
        
        df.to_csv(args.output, index=False, encoding='utf-8')
        print(f"\nДанные успешно сохранены в файл '{args.output}'")
        
        # Логируем статистику
        logging.info(f"Всего собрано уникальных событий: {len(df)}")
        logging.info("\nРаспределение по основным типам катастроф:")
        logging.info(df['Event Type'].value_counts().to_string())
        logging.info("\nРаспределение по подтипам катастроф:")
        logging.info(df['Event Subtype'].value_counts().to_string())
        logging.info("\nРаспределение по странам:")
        logging.info(df['Country'].value_counts().to_string())
        
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
            
        # Логируем успешное завершение работы
        logging.info("Парсер успешно завершил работу")
            
    except Exception as e:
        print(f"Ошибка при парсинге: {str(e)}")
        logging.error(f"Критическая ошибка при парсинге: {str(e)}")
        raise

if __name__ == "__main__":
    main() 