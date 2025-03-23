import pytest
from src.wikipedia_parser import WikipediaParser

def test_parser_initialization():
    parser = WikipediaParser()
    assert parser.base_url == "https://ru.wikipedia.org"

def test_get_page_content():
    parser = WikipediaParser()
    content = parser.get_page_content("Главная_страница")
    assert content is not None
    assert len(content) > 0

def test_parse_events():
    parser = WikipediaParser()
    events = parser.parse_events("Главная_страница")
    assert isinstance(events, list) 